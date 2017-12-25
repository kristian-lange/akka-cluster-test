package akka;

class DistributedWorkerSpec(_system: ActorSystem)
  extends TestKit(_system)
  with Matchers
  with FlatSpecLike
  with BeforeAndAfterAll
  with ImplicitSender {

  import DistributedWorkerSpec._

  val workTimeout = 3.seconds

  def this() = this(ActorSystem("DistributedWorkerSpec", DistributedWorkerSpec.clusterConfig))

  val backendSystem: ActorSystem = {
    val config = ConfigFactory.parseString("akka.cluster.roles=[master]").withFallback(clusterConfig)
    ActorSystem("DistributedWorkerSpec", config)
  }

  val workerSystem: ActorSystem = ActorSystem("DistributedWorkerSpec", clusterConfig)

  val storageLocations = List(
    "akka.persistence.journal.leveldb.dir",
    "akka.persistence.snapshot-store.local.dir").map(s => new File(system.settings.config.getString(s)))

  override def beforeAll(): Unit = {
    // make sure we do not use persisted data from a previous run
    storageLocations.foreach(dir => FileUtils.deleteDirectory(dir))
  }

  "Distributed workers" should "perform work and publish results" in {
    val clusterAddress = Cluster(backendSystem).selfAddress
    val clusterProbe = TestProbe()
    Cluster(backendSystem).subscribe(clusterProbe.ref, classOf[MemberUp])
    clusterProbe.expectMsgType[CurrentClusterState]
    Cluster(backendSystem).join(clusterAddress)
    clusterProbe.expectMsgType[MemberUp]

    backendSystem.actorOf(
      ClusterSingletonManager.props(
        Master.props(workTimeout),
        PoisonPill,
        ClusterSingletonManagerSettings(system).withRole("master")),
      "master")

    Cluster(workerSystem).join(clusterAddress)

    val masterProxy = workerSystem.actorOf(
      MasterSingleton.proxyProps(workerSystem),
      name = "masterProxy")
    val fastWorkerProps = Props(new Worker(masterProxy) {
      override def createScraper(): ActorRef = context.actorOf(Props(new FastWorkExecutor), "fast-executor")
    })

    for (n <- 1 to 3)
      workerSystem.actorOf(fastWorkerProps, "worker-" + n)

    val flakyWorkerProps = Props(new Worker(masterProxy) {
      override def createScraper(): ActorRef = {
        context.actorOf(Props(new FlakyWorkExecutor), "flaky-executor")
      }
    })
    val flakyWorker = workerSystem.actorOf(flakyWorkerProps, "flaky-worker")

    Cluster(system).join(clusterAddress)
    clusterProbe.expectMsgType[MemberUp]

    // allow posting work from the outside

    val frontend = system.actorOf(Props[RemoteControllableFrontend], "scraper-job-manager")

    val results = TestProbe()
    DistributedPubSub(system).mediator ! Subscribe(Master.ResultsTopic, results.ref)
    expectMsgType[SubscribeAck]

    // make sure pub sub topics are replicated over to the master system before triggering any work
    within(10.seconds) {
      awaitAssert {
        DistributedPubSub(backendSystem).mediator ! GetTopics
        expectMsgType[CurrentTopics].getTopics() should contain(Master.ResultsTopic)
      }
    }

    // make sure we can get one piece of work through to fail fast if it doesn't
    frontend ! JobOrder("1", 1)
    expectMsg("ok-1")
    within(10.seconds) {
      awaitAssert {
        results.expectMsgType[JobResult].jobId should be("1")
      }
    }


    // and then send in some actual work
    for (n <- 2 to 100) {
      frontend ! JobOrder(n.toString, n)
      expectMsg(s"ok-$n")
    }
    system.log.info("99 work items sent")

    results.within(20.seconds) {
      val ids = results.receiveN(99).map { case JobResult(workId, _) => workId }
      // nothing lost, and no duplicates
      ids.toVector.map(_.toInt).sorted should be((2 to 100).toVector)
    }

  }

  override def afterAll(): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    val allTerminated = Future.sequence(Seq(
      system.terminate(),
      backendSystem.terminate(),
      workerSystem.terminate()
    ))

    Await.ready(allTerminated, Duration.Inf)

    storageLocations.foreach(dir => FileUtils.deleteDirectory(dir))
  }

}
