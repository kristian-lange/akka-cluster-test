package worker

import java.util.UUID

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor._

import scala.concurrent.duration._

/**
  * The worker is actually more of a middle manager, delegating the actual work
  * to the WorkExecutor, supervising it and keeping itself available to interact with the work
  * master.
  */
object Worker {

  def props(masterProxy: ActorRef): Props = Props(new Worker(masterProxy))

}

class Worker(masterProxy: ActorRef)
    extends Actor with Timers with ActorLogging {

  import MasterWorkerProtocol._
  import context.dispatcher

  val workerId = UUID.randomUUID().toString
  val registerInterval = context.system.settings.config.getDuration("distributed-workers" +
      ".worker-registration-interval").getSeconds.seconds

  val registerTask = context.system.scheduler.schedule(0.seconds, registerInterval, masterProxy,
    RegisterWorker(workerId))

  val workExecutor = createScraper()

  var currentWorkId: Option[String] = None

  def workId: String = currentWorkId match {
    case Some(workId) => workId
    case None => throw new IllegalStateException("Not working")
  }

  override def preStart(): Unit = {
    masterProxy ! WorkerRequestsWork(workerId)
  }

  def receive = idle

  def idle: Receive = {
    case WorkIsAvailable =>
      // this is the only state where we reply to WorkIsReady
      masterProxy ! WorkerRequestsWork(workerId)

    case WorkOrder(id: String, profile: Profile) =>
      log.info("Got scrape work: {}", id.substring(0, 8))
      currentWorkId = Some(id)
      workExecutor ! Scraper.Scrape(profile)
      context.become(working)

    case WorkOrder(id: String, _) =>
      log.warning("I only work with profiles at this time. I can't accept work {}.",
        id.substring(0, 8))
  }

  def working: Receive = {
    case Scraper.Complete(profile) =>
      log.info("Scrape work is complete: {}", profile._id.substring(0, 8))
      masterProxy ! WorkIsDone(workerId, workId, profile)
      context.setReceiveTimeout(5.seconds)
      context.become(waitForWorkIsDoneAck(profile))

    case _: WorkOrder =>
      log.warning("Yikes. Master told me to do work, while I'm already working.")
  }

  def waitForWorkIsDoneAck(result: Any): Receive = {
    case Ack(id) if id == workId =>
      masterProxy ! WorkerRequestsWork(workerId)
      context.setReceiveTimeout(Duration.Undefined)
      context.become(idle)

    case ReceiveTimeout =>
      log.info("No ack from master, resending work result {}", workId.substring(0, 8))
      masterProxy ! WorkIsDone(workerId, workId, result)
  }

  def createScraper(): ActorRef =
  // in addition to starting the actor we also watch it, so that
  // if it stops this worker will also be stopped
    context.watch(context.actorOf(Scraper.props, "scraper"))

  override def supervisorStrategy = OneForOneStrategy() {
    case _: ActorInitializationException => Stop
    case _: Exception =>
      currentWorkId foreach { workId => masterProxy ! WorkFailed(workerId, workId) }
      context.become(idle)
      Restart
  }

  override def postStop(): Unit = {
    registerTask.cancel()
    masterProxy ! DeRegisterWorker(workerId)
  }

}
