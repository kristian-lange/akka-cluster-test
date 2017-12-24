package akka

import scala.collection.immutable.Queue


object JobState {

  def empty: JobState = JobState(
    pendingJobs = Queue.empty,
    jobsInProgress = Map.empty,
    acceptedBulkIds = Set.empty,
    doneJobIds = Set.empty)

  trait JobDomainEvent

  case class BulkOrderAccepted(bulkOrder: BulkOrder) extends JobDomainEvent

  case class JobStarted(jobId: String) extends JobDomainEvent

  case class JobCompleted(jobId: String, result: Any) extends JobDomainEvent

  case class WorkerFailed(jobId: String) extends JobDomainEvent

  case class WorkerTimedOut(jobId: String) extends JobDomainEvent

}

case class JobState private(private val pendingJobs: Queue[JobOrder],
                            private val jobsInProgress: Map[String, JobOrder],
                            private val acceptedBulkIds: Set[String],
                            private val doneJobIds: Set[String]) {

  import JobState._

  def hasJobs: Boolean = pendingJobs.nonEmpty

  def pendingJobsSize: Int = pendingJobs.size

  def nextJob: JobOrder = pendingJobs.head

  def isAccepted(bulkId: String): Boolean = acceptedBulkIds.contains(bulkId)

  def isInProgress(jobId: String): Boolean = jobsInProgress.contains(jobId)

  def isDone(jobId: String): Boolean = doneJobIds.contains(jobId)

  def updated(event: JobDomainEvent): JobState = event match {
    case BulkOrderAccepted(bulkOrder) ⇒
      copy(
        pendingJobs = pendingJobs ++ bulkOrder.bulk,
        acceptedBulkIds = acceptedBulkIds + bulkOrder.bulkId)

    case JobStarted(jobId) ⇒
      val (job, rest) = pendingJobs.dequeue
      require(jobId == job.jobId, s"JobStarted expected jobId $jobId == ${job.jobId}")
      copy(
        pendingJobs = rest,
        jobsInProgress = jobsInProgress + (jobId -> job))

    case JobCompleted(jobId, _) ⇒
      copy(
        jobsInProgress = jobsInProgress - jobId,
        doneJobIds = doneJobIds + jobId)

    case WorkerFailed(jobId) ⇒
      copy(
        pendingJobs = pendingJobs enqueue jobsInProgress(jobId),
        jobsInProgress = jobsInProgress - jobId)

    case WorkerTimedOut(jobId) ⇒
      copy(
        pendingJobs = pendingJobs enqueue jobsInProgress(jobId),
        jobsInProgress = jobsInProgress - jobId)
  }

}
