package akka

import scala.collection.immutable.Queue

case class JobOrder(jobId: String, job: Any)

case class BulkOrder(bulkId: String, bulk: Queue[JobOrder])

case class JobResult(jobId: String, result: Any)

case class Profile(_id: String, profileURL: String, scraperClass: String, var state: Int)

case class Portal(_class: String)
