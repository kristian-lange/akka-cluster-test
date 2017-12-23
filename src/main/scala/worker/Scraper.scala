package worker

import java.util.concurrent.ThreadLocalRandom

import akka.actor.{Actor, Props}

import scala.concurrent.duration._

object Scraper {

  def props = Props(new Scraper)

  case class Scrape(n: String)

  case class Complete(result: String)

}

class Scraper extends Actor {

  import Scraper._
  import context.dispatcher

  def receive = {
    case Scrape(url: String) =>
      val result = s"Scraping profile from $url"

      // simulate that the processing time varies
      val randomProcessingTime = ThreadLocalRandom.current.nextInt(1, 3).seconds
      context.system.scheduler.scheduleOnce(randomProcessingTime, sender(), Complete(result))
  }

}
