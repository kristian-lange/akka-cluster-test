package worker

import java.util.concurrent.ThreadLocalRandom

import akka.actor.{Actor, Props}

import scala.concurrent.duration._

/**
 * Work executor is the actor actually performing the work.
 */
object ProfileScraper {
  def props = Props(new ProfileScraper)

  case class DoWork(n: String)
  case class WorkComplete(result: String)
}

class ProfileScraper extends Actor {
  import ProfileScraper._
  import context.dispatcher

  def receive = {
    case DoWork(url: String) =>
      val result = s"profile of $url"

      // simulate that the processing time varies
      val randomProcessingTime = ThreadLocalRandom.current.nextInt(1, 3).seconds
      context.system.scheduler.scheduleOnce(randomProcessingTime, sender(), WorkComplete(result))
  }

}
