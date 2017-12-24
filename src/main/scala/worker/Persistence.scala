package worker

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}

object Persistence {
  def props: Props = Props(new Persistence)
}

class Persistence extends Actor with ActorLogging {

  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Subscribe(Master.ResultsTopic, self)

  def receive = {
    case _: DistributedPubSubMediator.SubscribeAck =>
    case JobResult(_, profile: Profile) =>
      log.info("Stored profile {} in DB", profile._id.substring(0, 8))
    case JobResult(jobId, _) =>
      log.warning("Can't store anything but profiles at this point. Can't accept job {}.",
        jobId.substring(0, 8))
  }

}
