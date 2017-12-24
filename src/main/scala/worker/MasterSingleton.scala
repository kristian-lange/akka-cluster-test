package worker

import akka.actor.{ActorSystem, PoisonPill}
import akka.cluster.singleton._

import scala.concurrent.duration._

object MasterSingleton {

  private val singletonName = "master"
  private val singletonRole = "master"

  def startSingleton(system: ActorSystem) = {
    val jobTimeout = system.settings.config.getDuration("distributed-workers.job-timeout")
        .getSeconds.seconds

    system.actorOf(
      ClusterSingletonManager.props(
        Master.props(jobTimeout),
        PoisonPill,
        ClusterSingletonManagerSettings(system).withRole(singletonRole)
      ),
      singletonName)
  }

  def proxyProps(system: ActorSystem) = ClusterSingletonProxy.props(
    settings = ClusterSingletonProxySettings(system).withRole(singletonRole),
    singletonManagerPath = s"/user/$singletonName")
}
