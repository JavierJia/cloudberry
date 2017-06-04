package edu.uci.ics.cloudberry.zion.experiment

import akka.actor.{Actor, ActorRef, PoisonPill}
import edu.uci.ics.cloudberry.zion.TInterval
import org.joda.time.DateTime
import play.api.Logger

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object Common {
  val reportLog = Logger("report")
  val workerLog = Logger("worker")

  case class QueryStat(targetMS: Int, estSlice: Int, actualMS: Int)

  case class ResultFromDB(mills: Long, sum: Int)

  object AlgoType extends Enumeration {
    type Type = Value
    val NormalGaussian = Value(1)
    val Histogram = Value(2)
    val Baseline = Value(3)
  }

  case class Parameters(reportInterval: Int,
                        algo: AlgoType.Value,
                        alpha: Double,
                        reporter: ActorRef,
                        keyword: String,
                        minHours: Int)

  case class HistoryStats(history: mutable.Builder[Common.QueryStat, List[Common.QueryStat]],
                          fullHistory: mutable.Builder[Common.QueryStat, List[Common.QueryStat]])

  class Reporter(keyword: String, limit: FiniteDuration)(implicit val ec : ExecutionContext) extends Actor {

    import Reporter._

    object Report

    var schedule = context.system.scheduler.schedule(limit, limit, self, Report)
    val startTime = DateTime.now()

    val queue: mutable.Queue[OneShot] = new mutable.Queue[OneShot]()

    var numReports = 0
    var numFailed = 0
    var delayed = 0l
    var sumResult = 0

    def hungry(since: DateTime): Actor.Receive = {
      case r: OneShot =>
        val delay = new TInterval(since, DateTime.now())
        delayed += delay.toDurationMillis
        sumResult += r.count
        reportLog.info(s"$keyword delayed ${delay.toDurationMillis / 1000.0}")
        schedule = context.system.scheduler.schedule(limit, limit, self, Report)
        context.unbecome()
      case Report =>
      // do nothing
      case any =>
        reportLog.warn(s"$keyword in hungry mode, don't know the message: $any")
    }

    override def receive = {
      case result: OneShot => queue.enqueue(result)
      case Report => {
        if (queue.isEmpty) {
          schedule.cancel()
          context.become(hungry(DateTime.now()), false)
          numFailed += 1
        } else {
          val result = queue.dequeue()
          sumResult += result.count
          reportLog.info(s"$keyword report result from ${result.start} of range ${result.range}")
        }
        numReports += 1
      }
      case Fin => {
        if (!queue.isEmpty) {
          val all = queue.dequeueAll(_ => true)
          reportLog.info(s"$keyword report result from ${all.map(_.start).head} of range ${all.map(_.range).sum}")
          numReports += 1
          sumResult += all.map(_.count).sum
        }
        val totalTime = DateTime.now().getMillis - startTime.getMillis
        reportLog.info(s"$keyword numOfReports: $numReports, sumTime: ${totalTime / 1000.0}, numOfFail: $numFailed, sumDelay: ${delayed / 1000.0}, sumCount:$sumResult")
        reportLog.info(s"=============FIN===============")
        self ! PoisonPill
      }
    }
  }

  object Reporter {

    case class OneShot(start: DateTime, range: Int, count: Int)

    case object Fin

  }

  def learnQueryState(start: DateTime, range: Int, estimate: Option[Long], actual: Long, state: HistoryStats) = {
    state.history += QueryStat(estimate.getOrElse(actual).toInt, range, actual.toInt)
    state.fullHistory += QueryStat(estimate.getOrElse(actual).toInt, range, actual.toInt)
  }


}
