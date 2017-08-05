package edu.uci.ics.cloudberry.zion.experiment

import akka.actor.{Actor, ActorRef, FSM, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import edu.uci.ics.cloudberry.zion.TInterval
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.json._

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.parsing.json.JSONObject

object ControlBackup extends App with Connection {

  import Common._

  object DBResultType extends Enumeration {
    type Type = Value
    val RISK = Value(1)
    val BACKUP = Value(2)
  }

  case class LabeledDBResult(label: DBResultType.Type, interval: TInterval, range: Int, estMills: Int, result: ResultFromDB)

  def runAQuery(query: String, label: DBResultType.Type, from: DateTime, to: DateTime, range: Int, estMills: Int): Future[LabeledDBResult] = {
    val start = DateTime.now
    adbConn.postQuery(query).map { ret =>
      val duration = DateTime.now.getMillis - start.getMillis
      val sum = (ret \\ "count").map(_.as[Int]).sum
      LabeledDBResult(label, new TInterval(from, to), range, estMills, ResultFromDB(duration, sum, ret))
    }
  }


  class Scheduler(val riskFullHistory: mutable.Builder[Common.QueryStat, List[Common.QueryStat]]) extends FSM[Scheduler.SchedulerState, Scheduler.SchedulerData] {

    import Scheduler._

    val backupFullHistory = List.newBuilder[QueryStat]

    var sumRisk = 0
    var sumBackup = 0

    var accResult: AccResult = null
    var accResultBeforeBackup: AccResult = null

    startWith(Idle, Uninitialized)

    // IDLE -- request(query,endTime,interval)
    // --> RISK  == receive request, send a query, set a timer, go to WAIT
    // --> WAIT  == receive db response, go to RISK with new request (query, endTime, interval)
    //           == receive time out, send Qbackup(), set a timer, go to PANIC
    // --> PANIC == receive result from Qrisk, go to RISK with new request(query, endTime, interval)
    //           == receive result from Qbackup
    //              == receive result from Qrisk, go to RISK with new request(query, endTime, interval)
    //              == receive result from time out, set timer, go to WAIT
    when(Idle) {
      case Event(request: Request, Uninitialized) =>
        val history = List.newBuilder[QueryStat]
        val historyStats = HistoryStats(history, riskFullHistory)

        val backupHistory = List.newBuilder[QueryStat]
        val backupHistoryStats = HistoryStats(backupHistory, backupFullHistory)
        goto(Risk) using StateData(request, historyStats, backupHistoryStats)
    }

    when(Risk) {
      case Event(FireRisk, StateData(r@Request(parameters, endTime, till, reportLimit), riskStats, backupStats)) =>

        workerLog.info(s"end: $endTime, till: $till")
        if (endTime.getMillis <= till.getMillis) {
          workerLog.info("@Risk DONE")
          reportLog.info(s"Sum risk:$sumRisk, backup:$sumBackup")
          parameters.reporter ! Reporter.Fin
          goto(Idle) using Uninitialized
        } else {

          sumRisk += 1
          workerLog.info(s"@Risk fire risk query, report limit: $reportLimit")

          // use full report limit first
          var waitingTimeOut = reportLimit
          val RangeAndEstTime(rRisk, estMills) = {
            val RangeAndEstTime(rRiskFull, estMillsFull) = decideRRiskAndESTTime(endTime, till, reportLimit, riskStats, parameters)
            if (parameters.withBackup) {
              if (rRiskFull > 5 * parameters.minHours) {
                waitingTimeOut = decideWaitTime(backupStats, reportLimit, parameters.withBackup)
                decideRRiskAndESTTime(endTime, till, waitingTimeOut, riskStats, parameters)
              } else {
                workerLog.info(s"@Risk not a risky case: $reportLimit, rRiskFull:$rRiskFull")
                RangeAndEstTime(rRiskFull, estMillsFull)
              }
            } else {
              RangeAndEstTime(rRiskFull, estMillsFull)
            }
          }

          debugLog.error(s"rRiskFull:$rRisk")
          val start = endTime.minusHours(rRisk)
          val sql = ResponseTime.getCountOnlyAQL(start, rRisk, toOpt(parameters.keyword))
          runAQuery(sql, DBResultType.RISK, start, endTime, rRisk, estMills) pipeTo self

          setTimer(WaitTimerName, WaitTimeOut, waitingTimeOut millis)
          goto(Waiting) using StateDataWithTimeOut(r, waitingTimeOut, false, riskStats, backupStats)
        }
    }

    when(Waiting) {
      case Event(r@LabeledDBResult(DBResultType.RISK, interval, range, estMills, result), StateDataWithTimeOut(request, waitTimeOut, _, riskStats, backupStats)) =>
        workerLog.info(s"@Waiting, receive response $r")
        val start = interval.getStart
        reportRiskSubAccBackup(request.parameters.reporter, start, range, result.sum, result.json)
        learnQueryState(start, range, Some(estMills), result.mills, riskStats)

        cancelTimer(WaitTimerName)

        // can't be overtime in waiting mode
        val nextLimit = request.parameters.reportInterval + request.reportLimit - result.mills

        goto(Risk) using StateData(request.copy(endTime = start, reportLimit = nextLimit.toInt), riskStats, backupStats)
      case Event(WaitTimeOut, _) =>
        workerLog.info(s"@Waiting, wait time out")
        goto(Panic)
    }

    def cancelBackupQuery(): Unit = {}

    def reportRiskSubAccBackup(reporter: ActorRef, start: DateTime, range: Int, sum: Int, json: JsValue): Unit = {
      if (accResultBeforeBackup == null) {
        if (accResult == null) {
          accResult = new AccResult(start, start.plusHours(range), sum, json.asInstanceOf[JsArray])
        } else {
          accResult.merge(start, start.plusHours(range), sum, json.asInstanceOf[JsArray])
        }
        reporter ! Reporter.OneShot(accResult.from, accResult.getRange, accResult.count, accResult.json)

      } else {
        //        assert(accBackup.to.minusHours(range) == start)
        //        assert(start.isBefore(accBackup.from))
        accResult = accResultBeforeBackup
        accResult.merge(start, start.plusHours(range), sum, json.asInstanceOf[JsArray])
        reporter ! Reporter.OneShot(accResult.from, accResult.getRange, accResult.count, accResult.json)
        accResultBeforeBackup = null
      }
    }

    when(Panic) {
      case Event(FireBackup, StateDataWithTimeOut(Request(parameters, endTime, till, reportLimit), waitTimeOut, false, riskStats, backupStats)) =>
        if (parameters.withBackup) {
          val panicLimit = reportLimit - waitTimeOut - 50
          if (panicLimit > 0) {
            sumBackup += 1
            workerLog.info(s"@Panic, fire backup query")
            val RangeAndEstTime(rBackup, estMills) = decideRBackAndESTTime(endTime, till, panicLimit, backupStats, parameters)

            val start = endTime.minusHours(rBackup)
            val sql = ResponseTime.getCountOnlyAQL(start, rBackup, toOpt(parameters.keyword))
            runAQuery(sql, DBResultType.BACKUP, start, endTime, rBackup, estMills) pipeTo self
            setTimer(PanicTimerName, PanicTimeOut, panicLimit millis)
          } else {
            self ! PanicTimeOut
          }
          stay
        } else {
          stay
        }
      case Event(r@LabeledDBResult(DBResultType.RISK, interval, range, estMills, result), s) =>
        workerLog.info(s"@Panic, receive risk response $r")
        val start = interval.getStart
        val data = s match {
          case ss: StateDataWithTimeOut => ss
          case ss: StateDataWithBackupResult => ss.stateDataWithTimeOut
        }
        reportRiskSubAccBackup(data.request.parameters.reporter, start, range, result.sum, result.json)
        learnQueryState(start, range, Some(estMills), result.mills, data.riskStats)

        cancelBackupQuery()
        cancelTimer(PanicTimerName)
        val nextLimit = if (data.panicTimeOut) {
          data.request.parameters.reportInterval
        } else {
          if (data.request.reportLimit > result.mills) { // only for the case of no backup queries
            data.request.parameters.reportInterval + data.request.reportLimit - result.mills
          } else {
            data.request.parameters.reportInterval
          }
        }
        workerLog.info(s"${data.request.parameters.reportInterval}, ${data.request.reportLimit}, ${result.mills} ")
        goto(Risk) using StateData(data.request.copy(endTime = start, reportLimit = nextLimit.toInt), data.riskStats, data.backupStats)

      case Event(r@LabeledDBResult(DBResultType.BACKUP, interval, range, estMills, result), s@StateDataWithTimeOut(request, waitTimeOut, isPanicTimeOut, riskStats, backupStats)) =>
        workerLog.info(s"@Panic, receive backup result $r")
        learnQueryState(interval.getStart, range, Some(estMills), result.mills, backupStats)
        if (interval.getEndMillis <= request.endTime.getMillis) {
          if (isPanicTimeOut) {

            val nextLimit = request.parameters.reportInterval
            val waitTimeOut = reportBackup(request.parameters.reporter, r, backupStats, nextLimit)

            goto(Waiting) using s.copy(request = request.copy(endTime = interval.getStart, reportLimit = nextLimit), waitTimeOut = waitTimeOut, panicTimeOut = false)

          } else {
            // hold the result
            stay using StateDataWithBackupResult(s, r)
          }
        } else {
          // receive from the previous backup query
          stay
        }
      case Event(PanicTimeOut, s: StateDataWithTimeOut) =>
        workerLog.info(s"@Panic, panic time out without any result")
        stay using s.copy(panicTimeOut = true)

      case Event(PanicTimeOut, StateDataWithBackupResult(state, backupResult)) =>
        workerLog.info(s"@Panic, panic time out with backup result $backupResult")
        val nextLimit = state.request.parameters.reportInterval
        val waitTimeOut = reportBackup(state.request.parameters.reporter, backupResult, state.backupStats, nextLimit)

        goto(Waiting) using state.copy(request = state.request.copy(endTime = backupResult.interval.getStart, reportLimit = nextLimit), waitTimeOut = waitTimeOut, panicTimeOut = false)
    }

    def reportBackup(reporter: ActorRef, dbResult: LabeledDBResult, backupStats: HistoryStats, nextLimit: Int): Int = {
      val start = dbResult.interval.getStart
      val range = dbResult.range

      if (accResultBeforeBackup == null) {
        if (accResult != null) {
          accResultBeforeBackup = accResult.copy()
        } else {
          accResultBeforeBackup = new AccResult(urEndDate, urEndDate, 0, JsArray(Seq.empty))
        }
      }
      if (accResult != null) {
        accResult.merge(start, dbResult.interval.getEnd, dbResult.result.sum, dbResult.result.json.asInstanceOf[JsArray])
      } else {
        accResult = new AccResult(start, dbResult.interval.getEnd, dbResult.result.sum, dbResult.result.json.asInstanceOf[JsArray])
      }

      reporter ! Reporter.OneShot(accResult.from, accResult.getRange, accResult.count, accResult.json)


      val waitingTimeOut = decideWaitTime(backupStats, nextLimit, true)
      setTimer(WaitTimerName, WaitTimeOut, waitingTimeOut millis)
      waitingTimeOut
    }

    onTransition {
      case any -> Idle =>
        accResult = null
        accResultBeforeBackup = null
      case any -> Risk =>
        workerLog.info(s"transition from $any to Risk")
        self ! FireRisk
      case Waiting -> Panic =>
        workerLog.info(s"transition from Waiting to Panic")
        self ! FireBackup
      case a -> b =>
        workerLog.error(s"WTF state transition from $a to $b")
    }

    whenUnhandled {
      case Event(CheckState, _) =>
        sender() ! stateName
        stay
      case Event(r@LabeledDBResult(DBResultType.BACKUP, interval, range, estMills, result), s: StateWithBackup) =>
        workerLog.error(s"$stateName received backup result $r")
        learnQueryState(interval.getStart, range, Some(estMills), result.mills, s.backupStats)
        stay
      case Event(Rewind, _) =>
        cancelTimer(WaitTimerName)
        cancelTimer(PanicTimerName)
        goto(Idle) using Uninitialized
      case Event(UpdateInterval(milli), s: SchedulerData) =>
        s match {
          case Uninitialized =>
            stay
          case data@StateData(request: Request, riskStats: HistoryStats, backupStats: HistoryStats) =>
            stay using data.copy(request = request.copy(parameters = request.parameters.copy(reportInterval = milli)))
          case data@StateDataWithTimeOut(request: Request, _, _, _, _) =>
            stay using data.copy(request = request.copy(parameters = request.parameters.copy(reportInterval = milli)))
          case data@StateDataWithBackupResult(stateDataWithTimeOut: StateDataWithTimeOut, backupResult: LabeledDBResult) =>
            stay using data.copy(stateDataWithTimeOut.copy(request = stateDataWithTimeOut.request.copy(parameters = stateDataWithTimeOut.request.parameters.copy(reportInterval = milli))))
        }
      case Event(UpdateWidth(widthHour), s: SchedulerData) =>
        s match {
          case Uninitialized =>
            stay
          case data@StateData(request: Request, riskStats: HistoryStats, backupStats: HistoryStats) =>
            stay using data.copy(request = request.copy(parameters = request.parameters.copy(width = widthHour)))
          case data@StateDataWithTimeOut(request: Request, _, _, _, _) =>
            stay using data.copy(request = request.copy(parameters = request.parameters.copy(width = widthHour)))
          case data@StateDataWithBackupResult(stateDataWithTimeOut: StateDataWithTimeOut, backupResult: LabeledDBResult) =>
            stay using data.copy(stateDataWithTimeOut.copy(request = stateDataWithTimeOut.request.copy(parameters = stateDataWithTimeOut.request.parameters.copy(width = widthHour))))
        }
      case Event(any, stateData) =>
        workerLog.error(s"WTF $stateName received msg $any, using data $stateData")
        stay
    }

    initialize()
  }

  object Scheduler {
    val WaitTimerName = "wait"
    val PanicTimerName = "panic"

    class AccBackup(var from: DateTime, var to: DateTime, var count: Int) {
      private var clear = true

      private def init(from: DateTime, to: DateTime, count: Int): Unit = {
        this.from = from
        this.to = to
        this.count = count
      }

      def acc(from: DateTime, to: DateTime, count: Int): Unit = {
        if (clear) {
          init(from, to, count)
          clear = false
        } else {
          assert(to == this.from)
          this.from = from
          this.count += count
        }
      }

      def reset() {
        clear = true
      }

      def isClear(): Boolean = clear
    }

    class AccResult(var from: DateTime, var to: DateTime, var count: Int, var json: JsArray) {

      def merge(from: DateTime, to: DateTime, count: Int, json: JsArray): Unit = {
        Logger.error(s"from:$from, to:$to, this.from:${this.from}, this.to:${this.to}")
        assert(to == this.from)
        this.from = from
        this.count += count
        //        this.json = mergeJSONArray(this.json, json, Seq("day"), "count")
        this.json = mergeCount(this.json, json)
      }

      def mergeByDayState(from: DateTime, to: DateTime, count: Int, json: JsArray): Unit = {
        Logger.error(s"from:$from, to:$to, this.from:${this.from}, this.to:${this.to}")
        assert(to == this.from)
        this.from = from
        this.count += count
        this.json = mergeJSONArray(this.json, json, Seq("day", "state"), "count")
        //        this.json = mergeJSONArray(this.json, json, Seq("day"), "count")
        //        this.json = mergeCount(this.json, json)
      }


      def getRange: Int = {
        new TInterval(from, to).toDuration.getStandardHours.toInt
      }

      def copy(): AccResult = {
        new AccResult(from, to, count, json)
      }
    }

    def mergeCount(jsLeft: JsArray, jsRight: JsArray): JsArray = {
      val count = (jsLeft \ "count").asOpt[Int].getOrElse(0) + (jsRight \ "count").asOpt[Int].getOrElse(0)
      JsArray(Seq(JsObject(Seq("count" -> JsNumber(count)))))
    }

    def mergeJSONArray(jsLeft: JsArray, jsRight: JsArray, keyIds: Seq[String], countField: String): JsArray = {
      val mapBuilder = mutable.HashMap.empty[Seq[JsValue], Int]
      insertOrUpdate(mapBuilder, jsLeft, keyIds, countField)
      insertOrUpdate(mapBuilder, jsRight, keyIds, countField)

      JsArray(mapBuilder.map { case (keys, count) =>
        val ks = keyIds.zip(keys).map { case (id, k) => id -> k }
        JsObject(ks ++ Seq(countField -> JsNumber(count)))
      }.toSeq)
    }

    def insertOrUpdate(map: mutable.Map[Seq[JsValue], Int], jsArray: JsArray, keyIds: Seq[String], countField: String): Unit = {
      jsArray.value.map { jsObj =>
        val keys = keyIds.map(k => (jsObj \ k).as[JsValue])
        val count = (jsObj \ countField).as[Int]
        map.get(keys) match {
          case Some(c) => map += keys -> (count + c)
          case None => map += keys -> count
        }
      }
    }

    case class Request(parameters: Parameters, endTime: DateTime, till: DateTime, reportLimit: Int)

    sealed trait SchedulerState

    case object Idle extends SchedulerState

    case object Risk extends SchedulerState

    case object Waiting extends SchedulerState

    case object Panic extends SchedulerState


    sealed trait SchedulerData

    case object Uninitialized extends SchedulerData

    trait StateWithBackup{
      val backupStats: HistoryStats
    }

    case class StateData(request: Request, riskStats: HistoryStats, backupStats: HistoryStats) extends SchedulerData with StateWithBackup

    case class StateDataWithTimeOut(request: Request, waitTimeOut: Int, panicTimeOut: Boolean, riskStats: HistoryStats, backupStats: HistoryStats) extends SchedulerData with StateWithBackup

    case class StateDataWithBackupResult(stateDataWithTimeOut: StateDataWithTimeOut, backupResult: LabeledDBResult) extends SchedulerData with StateWithBackup {
      override val backupStats: HistoryStats = stateDataWithTimeOut.backupStats
    }

    def toOpt(keyword: String): Option[String] = if (keyword.length > 0) Some(keyword) else None


    case class RangeAndEstTime(riskRange: Int, estMills: Int)

    def decideRRiskAndESTTime(endTime: DateTime, till: DateTime, limit: Int, historyStats: HistoryStats, parameters: Parameters): RangeAndEstTime = {
      if (parameters.useOneMain) {
        RangeAndEstTime(1900, 60000)
      } else {
        if (parameters.algo == AlgoType.EqualResultWidth) {
          return RangeAndEstTime(parameters.width, limit)
        }
        if (historyStats.history.result().isEmpty) {
          RangeAndEstTime(parameters.minHours, Int.MaxValue)
        } else {
          val (range, estTime) = ResponseTime.estimateInGeneral(limit, parameters.alpha, historyStats.history.result(), historyStats.fullHistory.result(), parameters.algo)
          RangeAndEstTime(range.toInt, estTime.toInt)
        }
      }
    }

    def decideWaitTime(backupStats: HistoryStats, reportLimit: Int, useBackup: Boolean): Int = {
      if (!useBackup) {
        return reportLimit
      }
      val result = backupStats.history.result()
      if (result.size > 0) {
//        val stats = result.sortBy(_.actualMS).take(result.size - 2) // remove some extreme case
        val stats = result
        val avg = stats.map(_.actualMS).sum / stats.size
        val variance = stats.map(s => (avg - s.actualMS) * (avg - s.actualMS)).sum / stats.size
        val o = Math.sqrt(variance)
        if (avg > reportLimit / 2) {
          //You are too slow, Don't try backup!
          workerLog.info(s"backup query is too expensive, give up backup!")
          reportLimit
        } else {
          val v = Math.max(reportLimit / 2, reportLimit - (avg + o)).toInt
          workerLog.info(s"waiting time out is: $v")
          v
        }
      } else {
        workerLog.info(s"waiting time out is just half ")
        reportLimit / 2
      }
    }

    def decideRBackAndESTTime(endTime: DateTime, till: DateTime, panicTimeOut: Int, backupStats: HistoryStats, parameters: Parameters): RangeAndEstTime = {
      RangeAndEstTime(parameters.minHours, 500)
    }

    case object WaitTimeOut

    case object PanicTimeOut

    case object CheckState

    case object FireRisk

    case object FireBackup

    case object Rewind

    case class UpdateInterval(milli: Int)

    case class UpdateWidth(widthHour: Int)

  }

  //// main class
  import scala.util.control.Breaks._


  def process: Unit = {

    import Scheduler._

    val globalHistory = List.newBuilder[QueryStat]
    val stream = this.getClass().getResourceAsStream("/count.history.log")
    Source.fromInputStream(stream).getLines().foreach { line =>
      globalHistory += QueryStat(0, 0, line.toInt)
    }

    val width = 60
    for (i <- 1 to 5) {
      for (alpha <- Seq(5, 15, 25, 50, 125)) {
//        for (alpha <- Seq(2.5)) {
        for (isGlobal <- Seq(false)) {

          for (algo <- Seq(AlgoType.NormalGaussian, AlgoType.Histogram)) {
//          for (algo <- Seq(AlgoType.EqualResultWidth)) {
            for (reportInterval <- Seq(2000)) {
              for (withBackup <- Seq(false)) {
                for (keyword <- Seq("zika", "election", "rain", "happy", "")) {
//                  for (keyword <- Seq( "happy")) {
                  val fullHistory = List.newBuilder[QueryStat]
                  if (isGlobal) {
                    fullHistory ++= globalHistory.result()
                  }
                  val start = fullHistory.result().size
                  val scheduler = system.actorOf(Props(new Scheduler(fullHistory)))
                  val reporter = system.actorOf(Props(new Reporter(keyword, reportInterval millis)))
                  scheduler ! Request(Parameters(reportInterval, algo, alpha, reporter, keyword, 1, width = (timeRange / width).toInt, withBackup = withBackup), urEndDate, urStartDate, reportInterval)
                  breakable {
                    while (true) {
                      implicit val timeOut: Timeout = Timeout(15 seconds)
                      (Await.result(scheduler ? CheckState, Duration.Inf)).asInstanceOf[SchedulerState] match {
                        case Idle =>
                          scheduler ! PoisonPill
                          workerLog.info(s"DONE $keyword, reportInterval:$reportInterval, withBackup: $withBackup")
                          Thread.sleep(5000)
                          break
                        case any =>
                          workerLog.info(s"CheckState is $any")
                          Thread.sleep(5000)
                      }
                    }
                  }
                  val history = fullHistory.result()
                  history.slice(start, history.length).foreach(stat => statsLog.info(s"$algo,$keyword,${stat.actualMS},${stat.targetMS},${stat.actualMS - stat.targetMS}"))
                  fullHistory.result().foreach(stat => statsLog.info(s"$algo,$keyword,${stat.actualMS},${stat.targetMS},${stat.actualMS - stat.targetMS}"))
                }
              }
            }
          }

        }
      }
    }
  }

  process
  exit()


}
