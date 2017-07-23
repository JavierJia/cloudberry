package edu.uci.ics.cloudberry.zion.experiment

import akka.actor.{Actor, ActorRef, PoisonPill}
import edu.uci.ics.cloudberry.zion.TInterval
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.json.{JsArray, JsNumber, JsObject, JsValue}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

object Common {
  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
  }

  val reportLog = Logger("report")
  val workerLog = Logger("worker")
  val statsLog = Logger("stats")
  val debugLog = Logger("debug")

  case class QueryStat(targetMS: Int, estSlice: Int, actualMS: Int)

  case class ResultFromDB(mills: Long, sum: Int, json: JsValue) {
    override def toString: String = s" ResultFromDB{mills:$mills, sum:$sum} "
  }

  object AlgoType extends Enumeration {
    type Type = Value
    val NormalGaussian = Value(1)
    val Histogram = Value(2)
    val Baseline = Value(3)
    // I am lazy
    val EqualResultWidth = Value(4)
  }

  case class Parameters(reportInterval: Int,
                        algo: AlgoType.Value,
                        alpha: Double,
                        reporter: ActorRef,
                        keyword: String,
                        minHours: Int,
                        width: Int = 0,
                        withBackup: Boolean = false,
                        useOneMain: Boolean = false
                       )

  case class HistoryStats(history: mutable.Builder[Common.QueryStat, List[Common.QueryStat]],
                          fullHistory: mutable.Builder[Common.QueryStat, List[Common.QueryStat]])

  class Reporter(keyword: String, var limit: FiniteDuration, outOpt: Option[ActorRef] = None)(implicit val ec: ExecutionContext) extends Actor {

    import Reporter._

    object Report

    var schedule = context.system.scheduler.schedule(limit, limit, self, Report)
    val startTime = DateTime.now()

    val queue: mutable.Queue[OneShot] = new mutable.Queue[OneShot]()

    var numReports = 0
    var numFailed = 0
    var delayed = 0l

    def toJson(mapBuilder: mutable.Map[JsValue, Int], keyField: String): JsArray = {
      JsArray(mapBuilder.map { case (key, count) =>
        JsObject(Seq(keyField -> key, "count" -> JsNumber(count)))
      }.toSeq)
    }

    def splitAndReport(out: ActorRef, json: JsValue): Unit = {
      val map = mutable.HashMap.empty[JsValue, Int]
      json.asInstanceOf[JsArray].value.foreach { case obj: JsObject =>
        val key = (obj \ "day").as[JsValue]
        val count = (obj \ "count").as[Int]
        map.get(key) match {
          case Some(c) => map.put(key, count + c)
          case None => map.put(key, count)
        }
      }
      val perDay = toJson(map, "day")
      map.clear()

      json.asInstanceOf[JsArray].value.foreach { case obj: JsObject =>
        val key = (obj \ "state").as[JsValue]
        val count = (obj \ "count").as[Int]
        map.get(key) match {
          case Some(c) => map.put(key, count + c)
          case None => map.put(key, count)
        }
      }
      val perState =  toJson(map, "state")
      out ! JsArray(Seq(perDay, perState))
    }

    def hungry(since: DateTime): Actor.Receive = {
      case UpdateInterval(l) =>
        limit = l
      case r: OneShot =>
        val delay = new TInterval(since, DateTime.now())
        delayed += delay.toDurationMillis
        outOpt.map(splitAndReport(_, r.json))
        reportLog.info(s"$keyword delayed ${delay.toDurationMillis / 1000.0}, range ${r.range}, count: ${r.count}")
        schedule = context.system.scheduler.schedule(limit, limit, self, Report)
        context.unbecome()
      case Report =>
        reportLog.info(s"$keyword has nothing to report")
      case any =>
        reportLog.warn(s"$keyword in hungry mode, don't know the message: $any")
    }

    override def receive = {
      case UpdateInterval(l) =>
        limit = l
      case result: OneShot => queue.enqueue(result)
      case Report => {
        if (queue.isEmpty) {
          schedule.cancel()
          context.become(hungry(DateTime.now()), false)
          numFailed += 1
        } else {
          val result = queue.dequeue()
          outOpt.map(splitAndReport(_, result.json))
          reportLog.info(s"$keyword report result from ${result.start} of range ${result.range}, count ${result.count}")
        }
        numReports += 1
      }
      case Fin => {
        if (queue.nonEmpty) {
          val all = queue.dequeueAll(_ => true)
          val sum = all.map(_.count).last
          reportLog.info(s"summary: $keyword report result from ${all.map(_.start).last} of range ${all.map(_.range).last}, count ${sum}")
          numReports += 1
          outOpt.map(splitAndReport(_, all.last.json))
        }
        val totalTime = DateTime.now().getMillis - startTime.getMillis
        reportLog.info(s"summary: $keyword numOfReports: $numReports, sumTime: ${totalTime / 1000.0}, numOfFail: $numFailed, sumDelay: ${delayed / 1000.0}")
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

    case class UpdateInterval(limit: FiniteDuration)

  }

  def learnQueryState(start: DateTime, range: Int, estimate: Option[Long], actual: Long, state: HistoryStats) = {
    state.history += QueryStat(estimate.getOrElse(actual).toInt, range, actual.toInt)
    state.fullHistory += QueryStat(estimate.getOrElse(actual).toInt, range, actual.toInt)
  }


}
