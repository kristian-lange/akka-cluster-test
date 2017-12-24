package worker

object WorkManagerMasterProtocol {

  case class MasterRequestsBulkOrder(id: String)

  case class Ack(id: String)

}
