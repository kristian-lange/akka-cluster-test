package akka

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}


/**
  * Used this as template: https://developer.lightbend.com/guides/akka-distributed-workers-scala
  * Blog post: http://letitcrash.com/post/29044669086/balancing-workload-across-nodes-with-akka-2
  *
  * JobManager (Actor)
  * - prepares bulk order (list of e.g. 1000 jobs with a page distribution according to page
  * priority): looks up DB (table scraperinfo for page(_class) and priority and table 'profile'
  * for URLs and state)
  * - profile has state ('link', 'update', 'pending', 'scraped'), 'lastScraped' (date) and
  * 'whenScraped' (list of dates)
  * - Master finds list of pendingJobs nearly empty and asks JobManager to send a new bulk job
  *
  * TODO master crashes: forever .Worker - No ack from master, resending job result: ok
  * TODO persistence with failover like master: ok
  * TODO work -> job (better worker != work): ok
  * TODO if master crashes -> kill jobmanager and persistence too
  * TODO master supervises job-manager and persistence - what if one dies
  * TODO worker supervises scraper
  * TODO new master starts -> in DB all profiles 'pending' to 'update'/'link'
  * TODO WARN  a.c.AutoDown - Don't use auto-down feature of Akka Cluster in production. See 'Auto-downing (DO NOT USE)' section of Akka Cluster documentation.
  * TODO logback file logging with rolling
  * TODO remove old jobs from JobState
  * TODO interface with scraper-scripts: 1) get bulk link profiles called from JobManager,
  *      2) write bulk into DB called from Persistence
  * TODO use Gradle
  * TODO tests
  */
object Main {

  // note that 2551 and 2552 are expected to be seed nodes though, even if
  // the Master starts at 2000
  val masterPortRange = 2000 to 2999

  def main(args: Array[String]): Unit = {
    args.headOption match {

      case None =>
        startClusterInSameJvm()

      case Some(portString) if portString.matches("""\d+""") =>
        val port = portString.toInt
        if (masterPortRange.contains(port)) startMaster(port)
        else startWorker(port, args.lift(1).map(_.toInt).getOrElse(1))
    }
  }

  def startClusterInSameJvm(): Unit = {
    // two master nodes
    startMaster(2551)
    startMaster(2552)
    // two worker nodes with two worker actors each
    startWorker(5001, 2)
    startWorker(5002, 2)
  }

  /**
    * Start a Master node on the given port.
    */
  def startMaster(port: Int): Unit = {
    val system = ActorSystem("ClusterSystem", config(port, "master"))
    MasterSingleton.startSingleton(system)
  }

  /**
    * Start a worker node, with n actual workers that will accept and process workloads
    */
  def startWorker(port: Int, workers: Int): Unit = {
    val system = ActorSystem("ClusterSystem", config(port, "worker"))
    val masterProxy = system.actorOf(
      MasterSingleton.proxyProps(system),
      name = "masterProxy")

    (1 to workers).foreach(n =>
      system.actorOf(Worker.props(masterProxy), s"worker-$n")
    )
  }

  def config(port: Int, role: String): Config =
    ConfigFactory.parseString(
      s"""
      akka.remote.netty.tcp.port=$port
      akka.cluster.roles=[$role]
    """).withFallback(ConfigFactory.load())

}
