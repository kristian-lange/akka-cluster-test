package worker

import akka._
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{
  CurrentTopics, GetTopics, Subscribe,
  SubscribeAck
}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.immutable.Queue
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object DistributedWorkerSpec {

  val clusterConfig = ConfigFactory.parseString(
    """
    akka {
      persistence {
        journal.plugin = "akka.persistence.journal.inmem"
        snapshot-store {
          plugin = "akka.persistence.snapshot-store.local"
          local.dir = "target/test-snapshots"
        }
      }
      extensions = ["akka.cluster.pubsub.DistributedPubSub"]
    }
    distributed-workers.consider-worker-dead-after = 10s
    distributed-workers.worker-registration-interval = 1s
    distributed-workers.bulk-order-size = 100
    distributed-workers.low-jobs-limit = 101
    """).withFallback(ConfigFactory.load())

  class FlakyScraper extends Actor with ActorLogging {
    var i = 0

    override def postRestart(reason: Throwable): Unit = {
      i = 3
      super.postRestart(reason)
    }

    def receive = {
      case Scraper.Scrape(profile: Profile) =>
        i += 1
        if (i == 3) {
          log.info("Cannot be trusted, crashing")
          throw new RuntimeException("Flaky worker")
        } else if (i == 5) {
          log.info("Cannot be trusted, stopping myself")
          context.stop(self)
        } else {
          profile.state = 4
          log.info("Cannot be trusted, but did complete job: {}", profile)
          sender() ! Scraper.Complete(profile)
        }
    }
  }

  class FastScraper extends Actor with ActorLogging {
    def receive = {
      case Scraper.Scrape(profile: Profile) =>
        profile.state = 4
        sender() ! Scraper.Complete(profile)
    }
  }

  class RemoteControllableJobManager extends JobManager {

    var currentBulkIdAndSender: Option[(String, ActorRef)] = None

    var jobIdCounter = 0
    var bulkIdCounter = 0

    override def nextProfile(): Profile = {
      userCounter += 1
      jobIdCounter += 1
      Profile(jobIdCounter.toString, "http://www.abc.com/" + userCounter, "AbcProfileScraper", 6)
    }

    override def generateBulkOrder() = {
      val bulk = Queue.fill(bulkOrderSize) {
        val profile = nextProfile()
        JobOrder(profile._id, profile)
      }
      bulkIdCounter += 1
      BulkOrder(bulkIdCounter.toString, bulk)
    }
  }

}

class DistributedWorkerSpec(_system: ActorSystem)
    extends TestKit(_system)
        with Matchers
        with FlatSpecLike
        with BeforeAndAfterAll
        with ImplicitSender {

  import DistributedWorkerSpec._

  val jobTimeout = 600.seconds

  def this() = this(ActorSystem("DistributedWorkerSpec", DistributedWorkerSpec.clusterConfig))

  val masterSystem: ActorSystem = {
    val config = ConfigFactory.parseString("akka.cluster.roles=[master]")
        .withFallback(clusterConfig)
    ActorSystem("DistributedWorkerSpec", config)
  }

  val workerSystem: ActorSystem = ActorSystem("DistributedWorkerSpec", clusterConfig)

  "Distributed workers" should "perform jobs and publish results" in {
    val clusterAddress = Cluster(masterSystem).selfAddress
    val clusterProbe = TestProbe()
    Cluster(masterSystem).subscribe(clusterProbe.ref, classOf[MemberUp])
    clusterProbe.expectMsgType[CurrentClusterState]
    Cluster(masterSystem).join(clusterAddress)
    clusterProbe.expectMsgType[MemberUp]

    val masterProps = Props(new Master(jobTimeout) {
      override def createJobManager(): ActorRef = context.actorOf(Props(new
              RemoteControllableJobManager), "job-manager")
    })

    masterSystem.actorOf(
      ClusterSingletonManager.props(
        masterProps,
        PoisonPill,
        ClusterSingletonManagerSettings(system).withRole("master")),
      "master")

    Cluster(workerSystem).join(clusterAddress)

    val masterProxy = workerSystem.actorOf(
      MasterSingleton.proxyProps(workerSystem),
      name = "masterProxy")
    val fastWorkerProps = Props(new Worker(masterProxy) {
      override def createScraper(): ActorRef = context.actorOf(Props(new FastScraper),
        "fast-executor")
    })

    for (n <- 1 to 3)
      workerSystem.actorOf(fastWorkerProps, "worker-" + n)

    val flakyWorkerProps = Props(new Worker(masterProxy) {
      override def createScraper(): ActorRef = {
        context.actorOf(Props(new FlakyScraper), "flaky-executor")
      }
    })
    val flakyWorker = workerSystem.actorOf(flakyWorkerProps, "flaky-worker")

    Cluster(system).join(clusterAddress)
    clusterProbe.expectMsgType[MemberUp]

    val results = TestProbe()
    DistributedPubSub(system).mediator ! Subscribe(Master.ResultsTopic, results.ref)
    expectMsgType[SubscribeAck]

    // make sure pub sub topics are replicated over to the master system before triggering any jobs
    within(100.seconds) {
      awaitAssert {
        DistributedPubSub(masterSystem).mediator ! GetTopics
        expectMsgType[CurrentTopics].getTopics() should contain(Master.ResultsTopic)
      }
    }

    within(100.seconds) {
      awaitAssert {
        results.expectMsgType[JobResult].jobId should be("1")
      }
    }

    results.within(200.seconds) {
      val ids = results.receiveN(99).map { case JobResult(jobId, _) => jobId }
      // nothing lost, and no duplicates
      ids.toVector.map(_.toInt).sorted should be((2 to 100).toVector)
    }

  }

  override def afterAll(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val allTerminated = Future.sequence(Seq(
      system.terminate(),
      masterSystem.terminate(),
      workerSystem.terminate()
    ))

    Await.ready(allTerminated, Duration.Inf)
  }

}
