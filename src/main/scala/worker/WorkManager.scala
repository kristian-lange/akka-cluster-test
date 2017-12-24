package worker

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props, Timers}
import akka.pattern._
import akka.util.Timeout

import scala.collection.immutable.Queue
import scala.concurrent.duration._

object WorkManager {

  def props: Props = Props(new WorkManager)

  val bulkOrderSize = 1000

  private case object NotOk

  private case object Retry

}

class WorkManager extends Actor with ActorLogging with Timers {

  import WorkManager._
  import context.dispatcher

  var userCounter = 0

  def nextId(): String = UUID.randomUUID().toString

  val dummyData = Vector(
    "LinkedinProfileScraper" -> "http://www.linkedin.com/",
    "XingProfileScraper" -> "http://www.xing.de/",
    "FacebookProfileScraper" -> "http://www.facebook.com/")

  val random = new scala.util.Random

  def nextProfile(): Profile = {
    userCounter += 1
    val d = dummyData(random.nextInt(dummyData.size))
    Profile(nextId(), d._2 + userCounter, d._1, 6)
  }

  def receive = idle

  def idle: Receive = {
    case WorkManagerMasterProtocol.MasterRequestsBulkOrder =>
      val bulkOrder = generateBulkOrder()
      context.become(busy(bulkOrder))
  }

  def busy(bulkOrderInProgress: BulkOrder): Receive = {
    sendWork(bulkOrderInProgress)

    {
      case WorkManagerMasterProtocol.Ack(bulkId) =>
        log.info("Got ack for bulk order {}", bulkId.substring(0, 8))
        context.become(idle)

      case NotOk =>
        log.info("Bulk work {} not accepted, retry after a while",
          bulkOrderInProgress.bulkId.substring(0, 8))
        timers.startSingleTimer("retry", Retry, 3.seconds)

      case Retry =>
        log.info("Retry sending bulk order {}", bulkOrderInProgress.bulkId.substring(0, 8))
        sendWork(bulkOrderInProgress)
    }
  }

  def sendWork(bulkWork: BulkOrder): Unit = {
    implicit val timeout = Timeout(5.seconds)
    (sender() ? bulkWork).recover {
      case _ => NotOk
    } pipeTo self
  }

  private def generateBulkOrder() = {
    val bulk = Queue.fill(bulkOrderSize) {
      val profile = nextProfile()
      WorkOrder(profile._id, profile)
    }
    BulkOrder(nextId(), bulk)
  }

}
