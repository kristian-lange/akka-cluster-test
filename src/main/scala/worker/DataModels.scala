package worker

import scala.collection.immutable.Queue

case class WorkOrder(id: String, work: Any)

case class BulkOrder(bulkId: String, bulk: Queue[WorkOrder])

case class WorkResult(id: String, result: Any)

case class Profile(_id: String, profileURL: String, scraperClass: String, var state: Int)
