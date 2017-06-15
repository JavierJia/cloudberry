package actor

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import akka.stream.Materializer
import edu.uci.ics.cloudberry.zion.experiment.Common.Reporter.Fin
import edu.uci.ics.cloudberry.zion.experiment.Common.{AlgoType, Parameters, Reporter}
import edu.uci.ics.cloudberry.zion.experiment.ControlBackup.Scheduler
import edu.uci.ics.cloudberry.zion.experiment.ControlBackup.Scheduler.{Request, Rewind, UpdateInterval, UpdateWidth}
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.json.JsValue

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

class ExperimentActor(out: ActorRef)(implicit ec: ExecutionContext, implicit val materializer: Materializer) extends Actor with ActorLogging {
  // build reporter
  // build scheduler
  import ExperimentActor._

  val scheduler = context.actorOf(Props(new Scheduler()))
  var curReporter: Option[ActorRef] = None

  override def receive: Receive = {
    case json: JsValue =>
      Logger.info("received:" + json.toString())
      (json \ "key").as[String] match {
        case s if s == keyRequest =>
          val reportInterval = (json \ "interval").as[Int]
          val keyword = (json \ "keywords").as[Seq[String]].headOption.getOrElse("")
          curReporter.map(_ ! PoisonPill)
          scheduler ! Rewind

          (json \ "type").as[String] match {
            case t if t == equalResult =>
              val resultWidth = reportInterval // in hour
              val reporter = context.actorOf(Props(new Reporter(keyword, 10 millis, Some(out))))
              curReporter = Some(reporter)
              // set a large report limit
              val reportLimit = 10 * 60 * 1000 // 10 mins
              scheduler ! Request(Parameters(reportLimit, AlgoType.EqualResultWidth, 1, reporter, keyword, 1, width = resultWidth), urEndDate, urStartDate, reportLimit)
            case t if t == equalResponse =>
              val reporter = context.actorOf(Props(new Reporter(keyword, reportInterval seconds, Some(out))))
              curReporter = Some(reporter)
              scheduler ! Request(Parameters(reportInterval * 1000, AlgoType.NormalGaussian, 1, reporter, keyword, 1), urEndDate, urStartDate, reportInterval * 1000)
            case t if t == minBackup =>
              ???
          }
        case s if s == keyUpdate =>
          (json \ "type").as[String] match {
            case t if t == equalResult =>
              val newHour = (json \ "interval").as[Int]
              scheduler ! UpdateWidth(newHour)
            case t if t == equalResponse =>
              val newInterval = (json \ "interval").as[Int]
              curReporter.map(_ ! Reporter.UpdateInterval(newInterval second))
              scheduler ! UpdateInterval(newInterval * 1000)
          }
      }
  }
}

object ExperimentActor {
  def props(out: ActorRef)(implicit ec: ExecutionContext, materializer: Materializer) = Props(new ExperimentActor(out))

  val keyRequest = "request"
  val keyUpdate = "update"
  val equalResult = "equal-result"
  val equalResponse = "equal-response"
  val minBackup = "min-backups"

  val urStartDate = new DateTime(2016, 11, 4, 15, 0)
  val urEndDate = new DateTime(2017, 1, 17, 6, 0)
}
