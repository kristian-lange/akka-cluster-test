package akka

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.singleton._

import scala.concurrent.duration._

object MasterSingleton {

  private val singletonName = "master"
  private val singletonRole = "master"

  def startSingleton(system: ActorSystem): ActorRef = {
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

  def proxyProps(system: ActorSystem): Props = ClusterSingletonProxy.props(
    settings = ClusterSingletonProxySettings(system).withRole(singletonRole),
    singletonManagerPath = s"/user/$singletonName")
}
