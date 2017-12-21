package worker

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import akka.actor.{Actor, ActorLogging, Cancellable, Props, Timers}
import akka.pattern._
import akka.util.Timeout

import scala.concurrent.duration._

/**
 * Dummy scraper-job-manager that periodically sends a workload to the master.
 */
object ScraperJobManager {

  def props: Props = Props(new ScraperJobManager)

  private case object NotOk
  private case object Tick
  private case object Retry
}

// #scraper-job-manager
class ScraperJobManager extends Actor with ActorLogging with Timers {
  import ScraperJobManager._
  import context.dispatcher

  val masterProxy = context.actorOf(
    MasterSingleton.proxyProps(context.system),
    name = "masterProxy")

  var userCounter = 0

  def nextWorkId(): String = UUID.randomUUID().toString

  val pages = Array("http://www.linkedin.com/", "http://www.xing.de/", "http://www.facebook.com/")

  val random = new scala.util.Random

  override def preStart(): Unit = {
    timers.startSingleTimer("tick", Tick, 5.seconds)
  }

  def receive = idle

  def idle: Receive = {
    case Tick =>
      userCounter += 1
      val nextUrl = pages(random.nextInt(3)) + userCounter
      log.info("Produced work: {}", nextUrl)
      val work = Work(nextWorkId(), nextUrl)
      context.become(busy(work))
  }

  def busy(workInProgress: Work): Receive = {
    sendWork(workInProgress)

    {
      case Master.Ack(workId) =>
        log.info("Got ack for workId {}", workId)
        val nextTick = ThreadLocalRandom.current.nextInt(3, 10).seconds
        timers.startSingleTimer(s"tick", Tick, nextTick)
        context.become(idle)

      case NotOk =>
        log.info("Work {} not accepted, retry after a while", workInProgress.workId)
        timers.startSingleTimer("retry", Retry, 3.seconds)

      case Retry =>
        log.info("Retrying work {}", workInProgress.workId)
        sendWork(workInProgress)
    }
  }

  def sendWork(work: Work): Unit = {
    implicit val timeout = Timeout(5.seconds)
    (masterProxy ? work).recover {
      case _ => NotOk
    } pipeTo self
  }

}
// #scraper-job-manager
