package worker

import scala.collection.immutable.Queue


object WorkState {

  def empty: WorkState = WorkState(
    pendingWork = Queue.empty,
    workInProgress = Map.empty,
    acceptedBulkWorkIds = Set.empty,
    doneWorkIds = Set.empty)

  trait WorkDomainEvent

  case class WorkAccepted(bulkWork: BulkWork) extends WorkDomainEvent

  case class WorkStarted(workId: String) extends WorkDomainEvent

  case class WorkCompleted(workId: String, result: Any) extends WorkDomainEvent

  case class WorkerFailed(workId: String) extends WorkDomainEvent

  case class WorkerTimedOut(workId: String) extends WorkDomainEvent

}

case class WorkState private(private val pendingWork: Queue[Work],
                             private val workInProgress: Map[String, Work],
                             private val acceptedBulkWorkIds: Set[String],
                             private val doneWorkIds: Set[String]) {

  import WorkState._

  def hasWork: Boolean = pendingWork.nonEmpty

  def isLowInWork: Boolean = pendingWork.size < WorkManager.bulkWorkSize / 10

  def nextWork: Work = pendingWork.head

  def isAccepted(bulkWorkId: String): Boolean = acceptedBulkWorkIds.contains(bulkWorkId)

  def isInProgress(workId: String): Boolean = workInProgress.contains(workId)

  def isDone(workId: String): Boolean = doneWorkIds.contains(workId)

  def updated(event: WorkDomainEvent): WorkState = event match {
    case WorkAccepted(bulkWork) ⇒
      copy(
        pendingWork = pendingWork ++ bulkWork.bulkJob,
        acceptedBulkWorkIds = acceptedBulkWorkIds + bulkWork.id)

    case WorkStarted(workId) ⇒
      val (work, rest) = pendingWork.dequeue
      require(workId == work.workId, s"WorkStarted expected workId $workId == ${work.workId}")
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
