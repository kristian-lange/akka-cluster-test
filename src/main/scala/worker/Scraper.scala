package worker

import java.util.concurrent.ThreadLocalRandom

import akka.actor.{Actor, ActorLogging, Props}

import scala.concurrent.duration._

object Scraper {

  def props = Props(new Scraper)

  case class Scrape(profile: Profile)

  case class Complete(profile: Profile)

}

class Scraper extends Actor with ActorLogging {

  import Scraper._
  import context.dispatcher

  def receive = {
    case Scrape(profile: Profile) =>
      log.info(s"Scraping profile from ${profile.profileURL} with ${profile.scraperClass}")
      profile.state = 4

      // simulate that the processing time varies
      val randomProcessingTime = ThreadLocalRandom.current.nextInt(1, 3).seconds
      context.system.scheduler.scheduleOnce(randomProcessingTime, sender(), Complete(profile))
  }

}
