package worker

object MasterWorkerProtocol {

  // Messages from Workers
  case class RegisterWorker(workerId: String)

  case class DeRegisterWorker(workerId: String)

  case class WorkerRequestsJob(workerId: String)

  case class JobIsDone(workerId: String, jobId: String, result: Any)

  case class JobFailed(workerId: String, jobId: String)

  // Messages to Workers
  case object JobIsAvailable

  case class Ack(jobId: String)

}
