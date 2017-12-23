package worker

import java.util.UUID

import akka.actor.{Actor, ActorLogging, Props, Timers}
import akka.pattern._
import akka.util.Timeout

import scala.collection.immutable.Queue
import scala.concurrent.duration._

object WorkManager {

  def props: Props = Props(new WorkManager)

  val bulkWorkSize = 1000

  private case object NotOk

  private case object Retry

}

class WorkManager extends Actor with ActorLogging with Timers {

  import WorkManager._
  import context.dispatcher

  var userCounter = 0

  def nextId(): String = UUID.randomUUID().toString

  val pages = Array("http://www.linkedin.com/", "http://www.xing.de/", "http://www.facebook.com/")

  val random = new scala.util.Random

  def receive = idle

  def idle: Receive = {
    case WorkManagerMasterProtocol.MasterRequestsBulkWork =>
      val bulkWork = produceBulkWork()
      context.become(busy(bulkWork))
  }

  def busy(bulkWorkInProgress: BulkWork): Receive = {
    sendWork(bulkWorkInProgress)

    {
      case WorkManagerMasterProtocol.Ack(id) =>
        log.info("Got ack for bulk work ID {}", id)
        context.become(idle)

      case NotOk =>
        log.info("Bulk work {} not accepted, retry after a while", bulkWorkInProgress.id)
        timers.startSingleTimer("retry", Retry, 3.seconds)

      case Retry =>
        log.info("Retry sending bulk work {}", bulkWorkInProgress.id)
        sendWork(bulkWorkInProgress)
    }
  }

  def sendWork(bulkWork: BulkWork): Unit = {
    implicit val timeout = Timeout(5.seconds)
    (sender() ? bulkWork).recover {
      case _ => NotOk
    } pipeTo self
  }

  private def produceBulkWork() = {
    val bulkJob = Queue.fill(bulkWorkSize) {
      userCounter += 1
      val nextUrl = pages(random.nextInt(3)) + userCounter
      Work(nextId(), nextUrl)
    }
    BulkWork(nextId(), bulkJob)
  }

}
