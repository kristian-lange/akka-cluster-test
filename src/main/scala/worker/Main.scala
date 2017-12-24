package worker

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}


/**
  * Used this as template: https://developer.lightbend.com/guides/akka-distributed-workers-scala
  * Blog post: http://letitcrash.com/post/29044669086/balancing-workload-across-nodes-with-akka-2
  *
  * WorkManager (Actor)
  * - prepares bulk work (list of e.g. 1000 work with a page distribution according to page
  * priority): looks up DB (table scraperinfo for page(_class) and priority and table 'profile'
  * for URLs and state)
  * - profile has state ('link', 'update', 'pending', 'scraped'), 'lastScraped' (date) and
  * 'whenScraped' (list of dates)
  * - Master finds list of pendingWork nearly empty and asks WorkManager to send a new bulk job
  *
  * TODO master crashes: forever .Worker - No ack from master, resending work result: ok
  * TODO persistence with failover like master
  * TODO work -> job (better worker != work)
  * TODO if master crashes -> kill workmanager and persistence too
  * TODO new master starts -> in DB all profiles 'pending' to 'update'/'link'
  * TODO WARN  a.c.AutoDown - Don't use auto-down feature of Akka Cluster in production. See 'Auto-downing (DO NOT USE)' section of Akka Cluster documentation.
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
