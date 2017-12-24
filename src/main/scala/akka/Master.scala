package akka

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}

import scala.concurrent.duration.{Deadline, FiniteDuration, _}

/**
  * The master actor keep tracks of all available workers, and all scheduled and ongoing work items
  */
object Master {

  val ResultsTopic = "results"

  val jobCheckIntervall = FiniteDuration(10, TimeUnit.SECONDS)

  def props(jobTimeout: FiniteDuration): Props = Props(new Master(jobTimeout))

  private sealed trait WorkerStatus

  private case object Idle extends WorkerStatus

  private case class Busy(jobId: String, deadline: Deadline) extends WorkerStatus

  private case class WorkerState(ref: ActorRef, status: WorkerStatus, staleWorkerDeadline: Deadline)

  private case object WorkerMaintenanceTick

  private case object JobCheckTick

}

class Master(jobTimeout: FiniteDuration) extends Actor with Timers with ActorLogging {

  import Master._
  import JobState._

  val considerWorkerDeadAfter: FiniteDuration =
    context.system.settings.config.getDuration("distributed-workers.consider-worker-dead-after")
        .getSeconds.seconds

  val lowJobsLimit = context.system.settings.config.getInt("distributed-workers.low-jobs-limit")

  def newStaleWorkerDeadline(): Deadline = considerWorkerDeadAfter.fromNow

  timers.startPeriodicTimer("worker-maintenance", WorkerMaintenanceTick, jobTimeout / 2)

  timers.startPeriodicTimer("jobs-check", JobCheckTick, jobCheckIntervall)

  // Start job-manager actor and watch it: if the actor crashes -> this master stops and
  // hopefully the other master takes over
  val jobManager = context.watch(context.actorOf(JobManager.props, "job-manager"))

  // Start persistence actor and watch it: if the actor crashes -> this master stops and
  // hopefully the other master takes over
  context.watch(context.actorOf(Persistence.props, "persistence"))

  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  private var workers = Map[String, WorkerState]()

  private var jobState = JobState.empty

  override def receive: Receive = {
    case MasterWorkerProtocol.RegisterWorker(workerId) =>
      if (workers.contains(workerId)) {
        workers += (workerId -> workers(workerId).copy(
          ref = sender(), staleWorkerDeadline = newStaleWorkerDeadline()
        ))
      } else {
        log.info("Worker registered: {}", workerId.substring(0, 8))
        val initialWorkerState = WorkerState(
          ref = sender(),
          status = Idle,
          staleWorkerDeadline = newStaleWorkerDeadline())
        workers += (workerId -> initialWorkerState)

        if (jobState.hasJobs)
          sender() ! MasterWorkerProtocol.JobIsAvailable
      }

    case MasterWorkerProtocol.DeRegisterWorker(workerId) =>
      workers.get(workerId) match {
        case Some(WorkerState(_, Busy(jobId, _), _)) =>
          // there was a workload assigned to the worker when it left
          log.info("Busy worker de-registered: {}", workerId.substring(0, 8))
          jobState = jobState.updated(WorkerFailed(jobId))
          notifyWorkers()
        case Some(_) =>
          log.info("Worker de-registered: {}", workerId.substring(0, 8))
        case _ =>
      }
      workers -= workerId

    case MasterWorkerProtocol.WorkerRequestsJob(workerId) =>
      if (jobState.hasJobs) {
        workers.get(workerId) match {
          case Some(workerState@WorkerState(_, Idle, _)) =>
            val job = jobState.nextJob
            jobState = jobState.updated(JobStarted(job.jobId))
            log.info("Giving worker {} job {}",
              workerId.substring(0, 8), job.jobId.substring(0, 8))
            val newWorkerState = workerState.copy(
              status = Busy(job.jobId, Deadline.now + jobTimeout),
              staleWorkerDeadline = newStaleWorkerDeadline())
            workers += (workerId -> newWorkerState)
            sender() ! job
          case _ =>
        }
      }

    case MasterWorkerProtocol.JobIsDone(workerId, jobId, result) =>
      // idempotent - redelivery from the worker may cause duplicates, so it needs to be
      if (jobState.isDone(jobId)) {
        // previous Ack was lost, confirm again that this is done
        sender() ! MasterWorkerProtocol.Ack(jobId)
      } else {
        if (jobState.isInProgress(jobId)) {
          log.info("Job {} completed by worker {}",
            jobId.substring(0, 8), workerId.substring(0, 8))
        } else {
          log.warning("Job {} isn't in progress at this master but reported as completed by " +
              "worker {}.", jobId.substring(0, 8), workerId.substring(0, 8))
        }
        changeWorkerToIdle(workerId, jobId)
        jobState = jobState.updated(JobCompleted(jobId, result))
        mediator ! DistributedPubSubMediator.Publish(ResultsTopic, JobResult(jobId, result))
        // Ack back to original sender
        sender ! MasterWorkerProtocol.Ack(jobId)
      }

    case MasterWorkerProtocol.JobFailed(workerId, jobId) =>
      if (jobState.isInProgress(jobId)) {
        log.info("Job {} failed by worker {}",
          jobId.substring(0, 8), workerId.substring(0, 8))
        changeWorkerToIdle(workerId, jobId)
        jobState = jobState.updated(WorkerFailed(jobId))
        notifyWorkers()
      }

    case bulkOrder: BulkOrder =>
      // idempotent
      if (jobState.isAccepted(bulkOrder.bulkId)) {
        sender() ! JobManagerMasterProtocol.Ack(bulkOrder.bulkId)
      } else {
        log.info("Accepted bulk order {}", bulkOrder.bulkId.substring(0, 8))
        // Ack back to original sender
        sender() ! JobManagerMasterProtocol.Ack(bulkOrder.bulkId)
        jobState = jobState.updated(BulkOrderAccepted(bulkOrder))
        notifyWorkers()
      }

    case WorkerMaintenanceTick =>
      workers.foreach {
        case (workerId, WorkerState(_, Busy(jobId, timeout), _)) if timeout.isOverdue() =>
          log.info("Job {} timed out: remove worker {}",
            jobId.substring(0, 8), workerId.substring(0, 8))
          workers -= workerId
          jobState = jobState.updated(WorkerTimedOut(jobId))
          notifyWorkers()

        case (workerId, WorkerState(_, Idle, lastHeardFrom)) if lastHeardFrom.isOverdue() =>
          log.info("Too long since heard from worker {}, pruning", workerId.substring(0, 8))
          workers -= workerId

        case _ => // this one is a keeper!
      }

    case JobCheckTick =>
      if (jobState.pendingJobsSize < lowJobsLimit) {
        log.info("Only {} jobs left: ask job-manager for more", jobState.pendingJobsSize)
        jobManager ! JobManagerMasterProtocol.MasterRequestsBulkOrder
      }
  }

  def notifyWorkers(): Unit =
    if (jobState.hasJobs) {
      workers.foreach {
        case (_, WorkerState(ref, Idle, _)) => ref ! MasterWorkerProtocol.JobIsAvailable
        case _ => // busy
      }
    }

  def changeWorkerToIdle(workerId: String, jobId: String): Unit =
    workers.get(workerId) match {
      case Some(workerState@WorkerState(_, Busy(`jobId`, _), _)) ⇒
        val newWorkerState = workerState.copy(status = Idle, staleWorkerDeadline =
            newStaleWorkerDeadline())
        workers += (workerId -> newWorkerState)
      case _ ⇒
      // ok, might happen after standby recovery, worker state is not persisted
    }

  def tooLongSinceHeardFrom(lastHeardFrom: Long): Boolean =
    System.currentTimeMillis() - lastHeardFrom > considerWorkerDeadAfter.toMillis

}
