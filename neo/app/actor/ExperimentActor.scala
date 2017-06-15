package actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.Materializer
import edu.uci.ics.cloudberry.zion.experiment.Common.Reporter.Fin
import edu.uci.ics.cloudberry.zion.experiment.Common.{AlgoType, Parameters, Reporter}
import edu.uci.ics.cloudberry.zion.experiment.ControlBackup.Scheduler
import edu.uci.ics.cloudberry.zion.experiment.ControlBackup.Scheduler.Request
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
  var curReporter : Option[ActorRef] = None

  override def receive: Receive = {
    case json: JsValue =>
      Logger.info("received:" + json.toString())
      (json \ "key").as[String] match {
        case s if s == keyRequest =>
          (json \ "type").as[String] match {
            case t if t == equalResult =>
            case t if t == equalResponse =>
              val reportInterval = (json \ "interval").as[Int]
              val keyword = (json \ "keywords").as[Seq[String]].headOption.getOrElse("")

              curReporter.map( _ ! Fin)
              val reporter = context.actorOf(Props(new Reporter(keyword, reportInterval seconds, Some(out))))
              scheduler ! Request(Parameters(reportInterval * 1000, AlgoType.Baseline, 1, reporter, keyword, 1), urEndDate, urStartDate, reportInterval*1000)
              curReporter = Some(reporter)
            case t if t == minBackup =>
              ???
          }
        case s if s == keyUpdate =>
          ???
      }
    //            val reporter = context.actorOf(Props(new Reporter(keyword, reportInterval millis)))
    //            scheduler ! Request(Parameters(reportInterval, AlgoType.Baseline, 1, reporter, keyword, 1), urEndDate, urStartDate, reportInterval)
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
