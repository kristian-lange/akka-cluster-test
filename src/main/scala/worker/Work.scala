package worker

import scala.collection.immutable.Queue

case class Work(workId: String, job: Any)

case class BulkWork(id: String, bulkJob: Queue[Work])

case class WorkResult(workId: String, result: Any)
