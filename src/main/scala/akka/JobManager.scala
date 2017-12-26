package akka

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props, Timers}
import akka.pattern._
import akka.util.Timeout

import scala.collection.immutable.Queue
import scala.concurrent.duration._

object JobManager {

  def props: Props = Props(new JobManager)

  private case object NotOk

  private case object Retry

}

class JobManager extends Actor with ActorLogging with Timers {

  import JobManager._
  import context.dispatcher

  val bulkOrderSize = context.system.settings.config.getInt("distributed-workers.bulk-order-size")

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

  def generateBulkOrder() = {
    val bulk = Queue.fill(bulkOrderSize) {
      val profile = nextProfile()
      JobOrder(profile._id, profile)
    }
    BulkOrder(nextId(), bulk)
  }

  def receive = idle

  def idle: Receive = {
    case JobManagerMasterProtocol.MasterRequestsBulkOrder =>
      val bulkOrder = generateBulkOrder()
      context.become(busy(bulkOrder))
  }

  def busy(bulkOrderInProgress: BulkOrder): Receive = {
    sendBulkOrder(bulkOrderInProgress)

    {
      case JobManagerMasterProtocol.Ack(bulkId) =>
        log.info("Got ack for bulk order {}", Utils.first8Chars(bulkId))
        context.become(idle)

      case NotOk =>
        log.info("Bulk order {} not accepted, retry after a while",
          Utils.first8Chars(bulkOrderInProgress.bulkId))
        timers.startSingleTimer("retry", Retry, 3.seconds)

      case Retry =>
        log.info("Retry sending bulk order {}", Utils.first8Chars(bulkOrderInProgress.bulkId))
        sendBulkOrder(bulkOrderInProgress)
    }
  }

  def sendBulkOrder(bulkOrder: BulkOrder): Unit = {
    implicit val timeout = Timeout(5.seconds)
    (sender() ? bulkOrder).recover {
      case _ => NotOk
    } pipeTo self
  }

}
