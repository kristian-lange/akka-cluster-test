package worker

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Timers}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}

import scala.concurrent.duration.{Deadline, FiniteDuration, _}

/**
  * The master actor keep tracks of all available workers, and all scheduled and ongoing work items
  */
object Master {

  val ResultsTopic = "results"

  def props(workTimeout: FiniteDuration): Props = Props(new Master(workTimeout))

  private sealed trait WorkerStatus

  private case object Idle extends WorkerStatus

  private case class Busy(workId: String, deadline: Deadline) extends WorkerStatus

  private case class WorkerState(ref: ActorRef, status: WorkerStatus, staleWorkerDeadline: Deadline)

  private case object CleanupTick

}

class Master(workTimeout: FiniteDuration) extends Actor with Timers with ActorLogging {

  import Master._
  import WorkState._

  val considerWorkerDeadAfter: FiniteDuration =
    context.system.settings.config.getDuration("distributed-workers.consider-worker-dead-after")
        .getSeconds.seconds

  def newStaleWorkerDeadline(): Deadline = considerWorkerDeadAfter.fromNow

  timers.startPeriodicTimer("cleanup", CleanupTick, workTimeout / 2)

  val mediator: ActorRef = DistributedPubSub(context.system).mediator

  val workManager = context.watch(context.actorOf(WorkManager.props, "workManager"))

  // the set of available workers is not event sourced as it depends on the current set of workers
  private var workers = Map[String, WorkerState]()

  // workState is event sourced to be able to make sure work is processed even in case of crash
  private var workState = WorkState.empty

  override def receive: Receive = {
    case MasterWorkerProtocol.RegisterWorker(workerId) =>
      if (workers.contains(workerId)) {
        workers += (workerId -> workers(workerId).copy(ref = sender(), staleWorkerDeadline =
            newStaleWorkerDeadline()))
      } else {
        log.info("Worker registered: {}", workerId)
        val initialWorkerState = WorkerState(
          ref = sender(),
          status = Idle,
          staleWorkerDeadline = newStaleWorkerDeadline())
        workers += (workerId -> initialWorkerState)

        if (workState.hasWork)
          sender() ! MasterWorkerProtocol.WorkIsReady
      }

    case MasterWorkerProtocol.DeRegisterWorker(workerId) =>
      workers.get(workerId) match {
        case Some(WorkerState(_, Busy(workId, _), _)) =>
          // there was a workload assigned to the worker when it left
          log.info("Busy worker de-registered: {}", workerId)
          workState = workState.updated(WorkerFailed(workId))
          notifyWorkers()
        case Some(_) =>
          log.info("Worker de-registered: {}", workerId)
        case _ =>
      }
      workers -= workerId

    case MasterWorkerProtocol.WorkerRequestsWork(workerId) =>
      if (workState.hasWork) {
        workers.get(workerId) match {
          case Some(workerState@WorkerState(_, Idle, _)) =>
            val work = workState.nextWork
            workState = workState.updated(WorkStarted(work.workId))
            log.info("Giving worker {} some work {}", workerId, work.workId)
            val newWorkerState = workerState.copy(
              status = Busy(work.workId, Deadline.now + workTimeout),
              staleWorkerDeadline = newStaleWorkerDeadline())
            workers += (workerId -> newWorkerState)
            sender() ! work
          case _ =>
        }
      }

    case MasterWorkerProtocol.WorkIsDone(workerId, workId, result) =>
      // idempotent - redelivery from the worker may cause duplicates, so it needs to be
      if (workState.isDone(workId)) {
        // previous Ack was lost, confirm again that this is done
        sender() ! MasterWorkerProtocol.Ack(workId)
      } else if (!workState.isInProgress(workId)) {
        log.info("Work {} not in progress, reported as done by worker {}", workId, workerId)
      } else {
        log.info("Work {} is done by worker {}", workId, workerId)
        changeWorkerToIdle(workerId, workId)
        workState = workState.updated(WorkCompleted(workId, result))
        mediator ! DistributedPubSubMediator.Publish(ResultsTopic, WorkResult(workId, result))
        // Ack back to original sender
        sender ! MasterWorkerProtocol.Ack(workId)
      }

    case MasterWorkerProtocol.WorkFailed(workerId, workId) =>
      if (workState.isInProgress(workId)) {
        log.info("Work {} failed by worker {}", workId, workerId)
        changeWorkerToIdle(workerId, workId)
        workState = workState.updated(WorkerFailed(workId))
        notifyWorkers()
      }

    case bulkWork: BulkWork =>
      // idempotent
      if (workState.isAccepted(bulkWork.id)) {
        sender() ! WorkManagerMasterProtocol.Ack(bulkWork.id)
      } else {
        log.info("Accepted bulk work: {}", bulkWork.id)
        // Ack back to original sender
        sender() ! WorkManagerMasterProtocol.Ack(bulkWork.id)
        workState = workState.updated(WorkAccepted(bulkWork))
        notifyWorkers()
      }

    case CleanupTick =>
      workers.foreach {
        case (workerId, WorkerState(_, Busy(workId, timeout), _)) if timeout.isOverdue() =>
          log.info("Work timed out: {}", workId)
          workers -= workerId
          workState = workState.updated(WorkerTimedOut(workId))
          notifyWorkers()

        case (workerId, WorkerState(_, Idle, lastHeardFrom)) if lastHeardFrom.isOverdue() =>
          log.info("Too long since heard from worker {}, pruning", workerId)
          workers -= workerId

        case _ => // this one is a keeper!
      }
      if (workState.isLowInWork) {
        workManager ! WorkManagerMasterProtocol.MasterRequestsBulkWork
      }
  }

  def notifyWorkers(): Unit =
    if (workState.hasWork) {
      workers.foreach {
        case (_, WorkerState(ref, Idle, _)) => ref ! MasterWorkerProtocol.WorkIsReady
        case _ => // busy
      }
    }

  def changeWorkerToIdle(workerId: String, workId: String): Unit =
    workers.get(workerId) match {
      case Some(workerState@WorkerState(_, Busy(`workId`, _), _)) ⇒
        val newWorkerState = workerState.copy(status = Idle, staleWorkerDeadline =
            newStaleWorkerDeadline())
        workers += (workerId -> newWorkerState)
      case _ ⇒
      // ok, might happen after standby recovery, worker state is not persisted
    }

  def tooLongSinceHeardFrom(lastHeardFrom: Long) =
    System.currentTimeMillis() - lastHeardFrom > considerWorkerDeadAfter.toMillis

}
