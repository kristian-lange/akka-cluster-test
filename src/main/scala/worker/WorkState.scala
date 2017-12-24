package worker

import scala.collection.immutable.Queue


object WorkState {

  def empty: WorkState = WorkState(
    pendingWork = Queue.empty,
    workInProgress = Map.empty,
    acceptedBulkIds = Set.empty,
    doneWorkIds = Set.empty)

  trait WorkDomainEvent

  case class BulkOrderAccepted(bulkOrder: BulkOrder) extends WorkDomainEvent

  case class WorkStarted(workId: String) extends WorkDomainEvent

  case class WorkCompleted(workId: String, result: Any) extends WorkDomainEvent

  case class WorkerFailed(workId: String) extends WorkDomainEvent

  case class WorkerTimedOut(workId: String) extends WorkDomainEvent

}

case class WorkState private(private val pendingWork: Queue[WorkOrder],
                             private val workInProgress: Map[String, WorkOrder],
                             private val acceptedBulkIds: Set[String],
                             private val doneWorkIds: Set[String]) {

  import WorkState._

  def hasWork: Boolean = pendingWork.nonEmpty

  def isLowInWork: Boolean = pendingWork.size < WorkManager.bulkOrderSize / 10

  def nextWork: WorkOrder = pendingWork.head

  def isAccepted(bulkId: String): Boolean = acceptedBulkIds.contains(bulkId)

  def isInProgress(workId: String): Boolean = workInProgress.contains(workId)

  def isDone(workId: String): Boolean = doneWorkIds.contains(workId)

  def updated(event: WorkDomainEvent): WorkState = event match {
    case BulkOrderAccepted(bulkOrder) ⇒
      copy(
        pendingWork = pendingWork ++ bulkOrder.bulk,
        acceptedBulkIds = acceptedBulkIds + bulkOrder.bulkId)

    case WorkStarted(workId) ⇒
      val (work, rest) = pendingWork.dequeue
      require(workId == work.id, s"WorkStarted expected workId $workId == ${work.id}")
      copy(
        pendingWork = rest,
        workInProgress = workInProgress + (workId -> work))

    case WorkCompleted(workId, result) ⇒
      copy(
        workInProgress = workInProgress - workId,
        doneWorkIds = doneWorkIds + workId)

    case WorkerFailed(workId) ⇒
      copy(
        pendingWork = pendingWork enqueue workInProgress(workId),
        workInProgress = workInProgress - workId)

    case WorkerTimedOut(workId) ⇒
      copy(
        pendingWork = pendingWork enqueue workInProgress(workId),
        workInProgress = workInProgress - workId)
  }

}
