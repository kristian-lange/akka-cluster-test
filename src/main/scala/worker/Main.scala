package worker

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

/**
  * From template https://developer.lightbend.com/guides/akka-distributed-workers-scala
  */
object Main {

  // note that 2551 and 2552 are expected to be seed nodes though, even if
  // the Master starts at 2000
  val masterPortRange = 2000 to 2999

  val scraperJobManagerPortRange = 3000 to 3999

  def main(args: Array[String]): Unit = {
    args.headOption match {

      case None =>
        startClusterInSameJvm()

      case Some(portString) if portString.matches("""\d+""") =>
        val port = portString.toInt
        if (masterPortRange.contains(port)) startMaster(port)
        else if (scraperJobManagerPortRange.contains(port)) startScraperJobManager(port)
        else startWorker(port, args.lift(1).map(_.toInt).getOrElse(1))
    }
  }

  def startClusterInSameJvm(): Unit = {
    // two backend nodes
    startMaster(2551)
    startMaster(2552)
    // two scraper-job-manager nodes
    startScraperJobManager(3000)
    startScraperJobManager(3001)
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
    * Start a ScraperJobManger node that will submit work to the master nodes
    */
  def startScraperJobManager(port: Int): Unit = {
    val system = ActorSystem("ClusterSystem", config(port, "profile-pipeline"))
    system.actorOf(ProfilePipeline.props, "profile-pipeline")
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
