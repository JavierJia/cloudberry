package edu.uci.ics.cloudberry.zion.experiment

import akka.actor.{Actor, ActorRef, FSM}
import akka.pattern.pipe
import edu.uci.ics.cloudberry.zion.TInterval
import edu.uci.ics.cloudberry.zion.experiment.ControlBackup.Scheduler.{SchedulerData, SchedulerState}
import org.joda.time.DateTime
import play.api.Logger

import scala.concurrent.Future
import scala.concurrent.duration._

object ControlBackup extends App with Connection {

  import Common._

  val workerLog = Logger("worker")

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
      LabeledDBResult(label, new TInterval(from, to), range, estMills, ResultFromDB(duration, sum))
    }
  }


  class Scheduler extends FSM[SchedulerState, SchedulerData] {

    import Scheduler._

    val riskFullHistory = List.newBuilder[QueryStat]
    val backupFullHistory = List.newBuilder[QueryStat]

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

    when(Waiting) {
      case Event(response: LabeledDBResult, StateDataWithTimeOut(request, waitTimeOut, _, riskStats, backupStats)) =>
        val start = response.interval.getStart
        request.parameters.reporter ! Reporter.OneShot(start, response.range, response.result.sum)
        learnQueryState(start, response.range, Some(response.estMills), response.result.mills, riskStats)

        cancelTimer(WaitTimerName)

        // can't be overtime in waiting mode
        val nextLimit = request.parameters.reportInterval + request.reportLimit - response.result.mills

        goto(Risk) using StateData(request.copy(endTime = start, reportLimit = nextLimit.toInt), riskStats, backupStats)
      case Event(WaitTimeOut, _) =>
        goto(Panic)
    }

    when(Panic) {
      case Event(r@LabeledDBResult(label, interval, range, estMills, result), s@StateDataWithTimeOut(request, waitTimeOut, isPanicTimeOut, riskStats, backupStats)) =>
        cancelTimer(PanicTimerName)
        label match {
          case DBResultType.RISK =>
            val start = interval.getStart
            request.parameters.reporter ! Reporter.OneShot(start, range, result.sum)
            learnQueryState(start, range, Some(estMills), result.mills, riskStats)

            val nextLimit = if (isPanicTimeOut) request.parameters.reportInterval else request.parameters.reportInterval + request.reportLimit - result.mills
            goto(Risk) using StateData(request.copy(endTime = start, reportLimit = nextLimit.toInt), riskStats, backupStats)

          case DBResultType.BACKUP if interval.getEndMillis <= request.endTime.getMillis => // receive from the previous backup query
            if (isPanicTimeOut) {

              val nextLimit = request.parameters.reportInterval
              val waitTimeOut = reportBackup(request.parameters.reporter, r, backupStats, nextLimit)

              goto(Waiting) using s.copy(request = request.copy(endTime = interval.getStart, reportLimit = nextLimit), waitTimeOut = waitTimeOut, panicTimeOut = false)

            } else {
              // hold the result
              stay using StateDataWithBackupResult(s, r)
            }

          case other =>
            workerLog.info(s"receive something different : $other")
            stay
        }
      case Event(PanicTimeOut, s: StateDataWithTimeOut) =>
        stay using s.copy(panicTimeOut = true)

      case Event(PanicTimeOut, StateDataWithBackupResult(state, backupResult)) =>
        val nextLimit = state.request.parameters.reportInterval
        val waitTimeOut = reportBackup(state.request.parameters.reporter, backupResult, state.backupStats, nextLimit)

        goto(Waiting) using state.copy(request = state.request.copy(endTime = backupResult.interval.getStart, reportLimit = nextLimit), waitTimeOut = waitTimeOut, panicTimeOut = false)
    }

    def reportBackup(reporter: ActorRef, dbResult: LabeledDBResult, backupStats: HistoryStats, nextLimit: Int): Int = {
      val start = dbResult.interval.getStart
      val range = dbResult.range
      reporter ! Reporter.OneShot(start, range, dbResult.result.sum)
      learnQueryState(start, range, Some(dbResult.estMills), dbResult.result.mills, backupStats)

      val waitingTimeOut = decideWaitTime(backupStats, nextLimit)
      setTimer(WaitTimerName, WaitTimeOut, waitingTimeOut millis)
      waitingTimeOut
    }

    onTransition {
      case _ -> Risk =>
        stateData match {
          case StateData(r @ Request(parameters, endTime, till, reportLimit), riskStats, backupStats) =>

            if (endTime.getMillis < till.getMillis) {
              parameters.reporter ! Reporter.Fin
              goto(Idle) using Uninitialized
            }

            val (rRisk, estMills) = decideRRiskAndESTTime(endTime, till, riskStats, parameters)

            val start = endTime.minusHours(rRisk)
            val sql = ResponseTime.getAQL(start, rRisk, toOpt(parameters.keyword))
            runAQuery(sql, DBResultType.RISK, start, endTime, rRisk, estMills) pipeTo self

            val waitingTimeOut = decideWaitTime(backupStats, reportLimit)
            setTimer(WaitTimerName, WaitTimeOut, waitingTimeOut millis)
            goto(Waiting) using StateDataWithTimeOut(r, waitingTimeOut, false, riskStats, backupStats)
          case any =>
            workerLog.error(s"WTF state : $any")
        }
      case Waiting -> Panic =>
        stateData match {
          case StateDataWithTimeOut(Request(parameters, endTime, till, reportLimit), waitTimeOut, false, riskStats, backupStats) =>

            val panicLimit = reportLimit - waitTimeOut - 50
            val (rBackup, estMills) = decideRBackAndESTTime(endTime, till, panicLimit, backupStats, parameters)

            val start = endTime.minusHours(rBackup)
            val sql = ResponseTime.getAQL(start, rBackup, toOpt(parameters.keyword))
            runAQuery(sql, DBResultType.BACKUP, start, endTime, rBackup, estMills) pipeTo self

            setTimer(PanicTimerName, PanicTimeOut, panicLimit millis)
          case any =>
            workerLog.error(s"WTF transition Waiting -> Panic hit unknown stateData: $any")
        }
      case a -> b =>
        workerLog.error(s"WTF state transition from $a to $b")
    }

    whenUnhandled {
      case Event(any, stateData) =>
        workerLog.error(s"WTF $stateName received msg $any, using data $stateData")
        stay
    }

    initialize()
  }

  object Scheduler {
    val WaitTimerName = "wait"
    val PanicTimerName = "panic"

    case class Request(parameters: Parameters, endTime: DateTime, till: DateTime, reportLimit: Int)

    sealed trait SchedulerState

    case object Idle extends SchedulerState

    case object Risk extends SchedulerState

    case object Waiting extends SchedulerState

    case object Panic extends SchedulerState


    sealed trait SchedulerData

    case object Uninitialized extends SchedulerData

    case class StateData(request: Request, riskStats: HistoryStats, backupStats: HistoryStats) extends SchedulerData

    case class StateDataWithTimeOut(request: Request, waitTimeOut: Int, panicTimeOut: Boolean, riskStats: HistoryStats, backupStats: HistoryStats) extends SchedulerData

    case class StateDataWithBackupResult(stateDataWithTimeOut: StateDataWithTimeOut, backupResult: LabeledDBResult) extends SchedulerData

    def toOpt(keyword: String): Option[String] = if (keyword.length > 0) Some(keyword) else None


    case class RangeAndEstTime(riskRange: Int, estMills: Int)

    def decideRRiskAndESTTime(endTime: DateTime, till: DateTime, historyStats: HistoryStats, parameters: Parameters): RangeAndEstTime = {
      RangeAndEstTime(new TInterval(till, endTime).toDuration.getStandardHours.toInt, 50000)
    }

    def decideWaitTime(backupStats: HistoryStats, reportLimit: Int): Int = {
      500
    }

    def decideRBackAndESTTime(endTime: DateTime, till: DateTime, panicTimeOut: Int, backupStats: HistoryStats, parameters: Parameters): RangeAndEstTime = {
      RangeAndEstTime(parameters.minHours, 500)
    }

    case object WaitTimeOut

    case object PanicTimeOut

  }

}
