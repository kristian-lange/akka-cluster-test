package akka

import java.util.UUID

import akka.actor.SupervisorStrategy.{Restart, Stop}
import akka.actor._

import scala.concurrent.duration._

/**
  * The worker is actually more of a middle manager, delegating the actual jobs
  * to the job executors, supervising it and keeping itself available to interact with the
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

  var currentJobId: Option[String] = None

  def jobId: String = currentJobId match {
    case Some(jobId) => jobId
    case None => throw new IllegalStateException("Not working")
  }

  override def preStart(): Unit = {
    masterProxy ! WorkerRequestsJob(workerId)
  }

  def receive = idle

  def idle: Receive = {
    case JobIsAvailable =>
      // This is the only state where we reply to JobIsAvailable
      masterProxy ! WorkerRequestsJob(workerId)

    case JobOrder(jobId: String, profile: Profile) =>
      log.info("Got scrape job {}", Utils.first8Chars(jobId))
      currentJobId = Some(jobId)
      workExecutor ! Scraper.Scrape(profile)
      context.become(working)

    case JobOrder(jobId: String, _) =>
      log.warning("I only work with profiles at this time. I can't accept job {}.",
        Utils.first8Chars(jobId))
  }

  def working: Receive = {
    case Scraper.Complete(profile) =>
      log.info("Scrape job {} complete", Utils.first8Chars(profile._id))
      masterProxy ! JobIsDone(workerId, jobId, profile)
      context.setReceiveTimeout(5.seconds)
      context.become(waitForJobIsDoneAck(profile))

    case JobOrder(jobId: String, _) =>
      log.warning("Yikes. Master told me to do job {}, while I'm already working.",
        Utils.first8Chars(jobId))
  }

  def waitForJobIsDoneAck(result: Any): Receive = {
    case Ack(id) if id == jobId =>
      masterProxy ! WorkerRequestsJob(workerId)
      context.setReceiveTimeout(Duration.Undefined)
      context.become(idle)

    case ReceiveTimeout =>
      log.info("No ack from master, resending job result {}", Utils.first8Chars(jobId))
      masterProxy ! JobIsDone(workerId, jobId, result)
  }

  def createScraper(): ActorRef =
  // in addition to starting the actor we also watch it, so that
  // if it stops this worker will also be stopped
    context.watch(context.actorOf(Scraper.props, "scraper"))

  override def supervisorStrategy = OneForOneStrategy() {
    case _: ActorInitializationException => Stop
    case _: Exception =>
      currentJobId foreach { jobId => masterProxy ! JobFailed(workerId, jobId) }
      context.become(idle)
      Restart
  }

  override def postStop(): Unit = {
    registerTask.cancel()
    masterProxy ! DeRegisterWorker(workerId)
  }

}
