package worker

object JobManagerMasterProtocol {

  case class MasterRequestsBulkOrder(bulkId: String)

  case class Ack(bulkId: String)

}
