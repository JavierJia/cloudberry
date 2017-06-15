package edu.uci.ics.cloudberry.zion.experiment

import akka.actor.{Actor, ActorRef, PoisonPill}
import edu.uci.ics.cloudberry.zion.TInterval
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.json.{JsArray, JsValue}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object Common {
  val reportLog = Logger("report")
  val workerLog = Logger("worker")

  case class QueryStat(targetMS: Int, estSlice: Int, actualMS: Int)

  case class ResultFromDB(mills: Long, sum: Int, json: JsValue)

  object AlgoType extends Enumeration {
    type Type = Value
    val NormalGaussian = Value(1)
    val Histogram = Value(2)
    val Baseline = Value(3)
    // I am lazy
//    val EqualResultWidth = Value(4)
  }

  case class Parameters(reportInterval: Int,
                        algo: AlgoType.Value,
                        alpha: Double,
                        reporter: ActorRef,
                        keyword: String,
                        minHours: Int)

  case class HistoryStats(history: mutable.Builder[Common.QueryStat, List[Common.QueryStat]],
                          fullHistory: mutable.Builder[Common.QueryStat, List[Common.QueryStat]])

  class Reporter(keyword: String, limit: FiniteDuration, outOpt: Option[ActorRef]= None)(implicit val ec : ExecutionContext) extends Actor {

    import Reporter._

    object Report

    var schedule = context.system.scheduler.schedule(limit, limit, self, Report)
    val startTime = DateTime.now()

    val queue: mutable.Queue[OneShot] = new mutable.Queue[OneShot]()

    var numReports = 0
    var numFailed = 0
    var delayed = 0l
    val sumResultBuilder = Seq.newBuilder[Int]
    var json = JsArray(Seq.empty)

    def hungry(since: DateTime): Actor.Receive = {
      case r: OneShot =>
        val delay = new TInterval(since, DateTime.now())
        delayed += delay.toDurationMillis
        sumResultBuilder += r.count
        json = json ++ r.json.asInstanceOf[JsArray]
        outOpt.map(_ ! json)
        reportLog.info(s"$keyword delayed ${delay.toDurationMillis / 1000.0}, range ${r.range}, count: ${r.count}")
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
          sumResultBuilder += result.count
          json = json ++ result.json.asInstanceOf[JsArray]
          outOpt.map(_ ! json)
          reportLog.info(s"$keyword report result from ${result.start} of range ${result.range}, count ${result.count}")
        }
        numReports += 1
      }
      case Fin => {
        if (queue.nonEmpty) {
          val all = queue.dequeueAll(_ => true)
          val sum = all.map(_.count).sum
          reportLog.info(s"$keyword report result from ${all.map(_.start).last} of range ${all.map(_.range).last}, count ${sum}")
          numReports += 1
          sumResultBuilder += sum
          json = json ++ all.map(_.json.asInstanceOf[JsArray]).reduce((l,s) => l ++ s)
          outOpt.map(_ ! json)
        }
        val totalTime = DateTime.now().getMillis - startTime.getMillis
        val sumResult = sumResultBuilder.result()
        val avg = sumResult.sum / sumResult.size.toDouble
        val variance = sumResult.map( c => (c-avg)*(c-avg)).sum / sumResult.size
        val o = Math.sqrt(variance)
        reportLog.info(s"$keyword numOfReports: $numReports, sumTime: ${totalTime / 1000.0}, numOfFail: $numFailed, sumDelay: ${delayed / 1000.0}, sumCount:${sumResult.sum}, countVar: $variance, std:$o")
        reportLog.info(s"=============FIN===============")
        self ! PoisonPill
      }
      case any =>
        reportLog.error(s"unknown msg: $any")
    }
  }

  object Reporter {

    case class OneShot(start: DateTime, range: Int, count: Int, json: JsValue)

    case object Fin

  }

  def learnQueryState(start: DateTime, range: Int, estimate: Option[Long], actual: Long, state: HistoryStats) = {
    state.history += QueryStat(estimate.getOrElse(actual).toInt, range, actual.toInt)
    state.fullHistory += QueryStat(estimate.getOrElse(actual).toInt, range, actual.toInt)
  }


}
