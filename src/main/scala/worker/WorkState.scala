package worker

object WorkState {

  def empty: WorkState = WorkState(
    workInProgress = Map.empty,
    acceptedWorkIds = Set.empty,
    doneWorkIds = Set.empty)

  trait WorkDomainEvent
  // #events
  case class WorkStarted(work: Work) extends WorkDomainEvent
  case class WorkCompleted(workId: String, result: Any) extends WorkDomainEvent
  case class WorkerFailed(workId: String) extends WorkDomainEvent
  case class WorkerTimedOut(workId: String) extends WorkDomainEvent
  // #events
}

case class WorkState private (
  private val workInProgress: Map[String, Work],
  private val acceptedWorkIds: Set[String],
  private val doneWorkIds: Set[String]) {

  import WorkState._

  def isAccepted(workId: String): Boolean = acceptedWorkIds.contains(workId)
  def isInProgress(workId: String): Boolean = workInProgress.contains(workId)
  def isDone(workId: String): Boolean = doneWorkIds.contains(workId)

  def updated(event: WorkDomainEvent): WorkState = event match {

    case WorkStarted(work) ⇒
      copy(
        workInProgress = workInProgress + (work.workId -> work))

    case WorkCompleted(workId, result) ⇒
      copy(
        workInProgress = workInProgress - workId,
        doneWorkIds = doneWorkIds + workId)

    case WorkerFailed(workId) ⇒
      copy(
        workInProgress = workInProgress - workId)

    case WorkerTimedOut(workId) ⇒
      copy(
        workInProgress = workInProgress - workId)
  }

}
