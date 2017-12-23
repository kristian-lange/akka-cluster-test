package worker

object WorkManagerMasterProtocol {

  case class MasterRequestsBulkWork(id: String)

  case class Ack(id: String)

}
