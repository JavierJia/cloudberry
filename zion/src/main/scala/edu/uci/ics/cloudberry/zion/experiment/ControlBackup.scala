package edu.uci.ics.cloudberry.zion.experiment

import akka.actor.{Actor, ActorRef, FSM, PoisonPill, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import edu.uci.ics.cloudberry.zion.TInterval
import edu.uci.ics.cloudberry.zion.experiment.Common.AlgoType
import org.joda.time.DateTime
import play.api.libs.json.JsValue

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

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


  class Scheduler extends FSM[Scheduler.SchedulerState, Scheduler.SchedulerData] {

    import Scheduler._

    val riskFullHistory = List.newBuilder[QueryStat]
    val backupFullHistory = List.newBuilder[QueryStat]

    var sumRisk = 0
    var sumBackup = 0

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
          val waitingTimeOut = decideWaitTime(backupStats, reportLimit)
          val RangeAndEstTime(rRisk, estMills) = decideRRiskAndESTTime(endTime, till, waitingTimeOut, riskStats, parameters)

          val start = endTime.minusHours(rRisk)
          val sql = ResponseTime.getAQL(start, rRisk, toOpt(parameters.keyword))
          runAQuery(sql, DBResultType.RISK, start, endTime, rRisk, estMills) pipeTo self

          setTimer(WaitTimerName, WaitTimeOut, waitingTimeOut millis)
          goto(Waiting) using StateDataWithTimeOut(r, waitingTimeOut, false, riskStats, backupStats)
        }
    }

    when(Waiting) {
      case Event(r@LabeledDBResult(DBResultType.RISK, interval, range, estMills, result), StateDataWithTimeOut(request, waitTimeOut, _, riskStats, backupStats)) =>
        workerLog.info(s"@Waiting, receive response $r")
        val start = interval.getStart
        reportRiskSubAccBackup(request.parameters.reporter , start, range, result.sum, result.json)
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
      if(accBackup.isClear()) {
        reporter ! Reporter.OneShot(start, range, sum, json)
      } else {
        assert(accBackup.to.minusHours(range) == start )
        assert(start.isBefore(accBackup.from))
        //FIXME
        reporter ! Reporter.OneShot(start, new TInterval(start, accBackup.from).toDuration.getStandardHours.toInt, sum - accBackup.count, json)
        accBackup.reset()
      }
    }

    when(Panic) {
      case Event(FireBackup, StateDataWithTimeOut(Request(parameters, endTime, till, reportLimit), waitTimeOut, false, riskStats, backupStats)) =>
        sumBackup += 1
        workerLog.info(s"@Panic, fire backup query")
        val panicLimit = reportLimit - waitTimeOut - 50
        val RangeAndEstTime(rBackup, estMills) = decideRBackAndESTTime(endTime, till, panicLimit, backupStats, parameters)

        val start = endTime.minusHours(rBackup)
        val sql = ResponseTime.getAQL(start, rBackup, toOpt(parameters.keyword))
        runAQuery(sql, DBResultType.BACKUP, start, endTime, rBackup, estMills) pipeTo self

        setTimer(PanicTimerName, PanicTimeOut, panicLimit millis)
        stay
      case Event(r@LabeledDBResult(DBResultType.RISK, interval, range, estMills, result), s) =>
        workerLog.info(s"@Panic, receive risk response $r")
        val start = interval.getStart
        val data = s match {
          case ss: StateDataWithTimeOut => ss
          case ss: StateDataWithBackupResult => ss.stateDataWithTimeOut
        }
        reportRiskSubAccBackup( data.request.parameters.reporter, start, range, result.sum, result.json)
        learnQueryState(start, range, Some(estMills), result.mills, data.riskStats)

        cancelBackupQuery()
        cancelTimer(PanicTimerName)
        val nextLimit = if (data.panicTimeOut) {
          data.request.parameters.reportInterval
        } else {
          data.request.parameters.reportInterval + data.request.reportLimit - result.mills
        }
        workerLog.info(s"${data.request.parameters.reportInterval}, ${data.request.reportLimit}, ${result.mills} ")
        goto(Risk) using StateData(data.request.copy(endTime = start, reportLimit = nextLimit.toInt), data.riskStats, data.backupStats)

      case Event(r @ LabeledDBResult(DBResultType.BACKUP, interval, range, estMills, result), s@StateDataWithTimeOut(request, waitTimeOut, isPanicTimeOut, riskStats, backupStats)) =>
        workerLog.info(s"@Panic, receive backup result $r")
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
      reporter ! Reporter.OneShot(start, range, dbResult.result.sum, dbResult.result.json)
      learnQueryState(start, range, Some(dbResult.estMills), dbResult.result.mills, backupStats)

      accBackup.acc(start, dbResult.interval.getEnd, dbResult.result.sum)

      val waitingTimeOut = decideWaitTime(backupStats, nextLimit)
      setTimer(WaitTimerName, WaitTimeOut, waitingTimeOut millis)
      waitingTimeOut
    }

    onTransition {
      case any -> Risk =>
        workerLog.info(s"transition from $any to Risk")
        self ! FireRisk
      case Waiting -> Panic =>
        workerLog.info(s"transition from Waiting to Panic")
        if (withBackup) {
          self ! FireBackup
        }
      case a -> b =>
        workerLog.error(s"WTF state transition from $a to $b")
    }

    whenUnhandled {
      case Event(CheckState, _) =>
        sender() ! stateName
        stay
      case Event(r@LabeledDBResult(DBResultType.BACKUP, _, _, _, _), _) =>
        workerLog.error(s"$stateName received backup result $r")
        stay
      case Event(any, stateData) =>
        workerLog.error(s"WTF $stateName received msg $any, using data $stateData")
        stay
    }

    val accBackup = new AccBackup(DateTime.now(), DateTime.now(), 0)

    initialize()
  }

  object Scheduler {
    val WaitTimerName = "wait"
    val PanicTimerName = "panic"

    class AccBackup(var from: DateTime, var to: DateTime, var count : Int) {
      private var clear = true

      private def init(from: DateTime, to: DateTime, count: Int): Unit ={
        this.from = from
        this.to = to
        this.count = count
      }

      def acc(from : DateTime, to: DateTime, count: Int): Unit = {
        if (clear){
          init(from, to, count)
          clear = false
        } else {
          assert(to == this.from)
          this.from = from
          this.count += count
        }
      }

      def reset() {clear = true}
      def isClear():Boolean = clear
    }


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

    def decideRRiskAndESTTime(endTime: DateTime, till: DateTime, limit: Int, historyStats: HistoryStats, parameters: Parameters): RangeAndEstTime = {
      if (oneRisk) {
        RangeAndEstTime(1900, 50000)
      } else {
        if (historyStats.history.result().isEmpty) {
          RangeAndEstTime(parameters.minHours, Int.MaxValue)
        } else {
          val (range, estTime) = ResponseTime.estimateInGeneral(limit, alpha, historyStats.history.result(), historyStats.fullHistory.result(), parameters.algo)
          RangeAndEstTime(range.toInt, estTime.toInt)
        }
      }
    }

    def decideWaitTime(backupStats: HistoryStats, reportLimit: Int): Int = {
      if (!withBackup) {
        return reportLimit
      }
      val result = backupStats.history.result()
      if (result.size > 3) {
        val stats = result.sortBy(_.actualMS).take(result.size - 2) // remove some extreme case
        val avg = stats.map(_.actualMS).sum / stats.size
        val variance = stats.map(s => (avg - s.actualMS) * (avg - s.actualMS)).sum / stats.size
        val o = Math.sqrt(variance)
        val v = Math.max(reportLimit / 2, reportLimit - (avg + o)).toInt
        workerLog.info(s"waiting time out is: $v")
        v
      } else {
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

  }

  //// main class
  import scala.util.control.Breaks._


  val oneRisk = false
  val withBackup = false
  val alpha = 1

  def process: Unit = {
    import Scheduler._
    val reportInterval = 4000
    for (keyword <- Seq("clinton","election")) {
      val scheduler = system.actorOf(Props(new Scheduler()))
      val reporter = system.actorOf(Props(new Reporter(keyword, reportInterval millis)))
      scheduler ! Request(Parameters(reportInterval, AlgoType.Baseline, 1, reporter, keyword, 1), urEndDate, urStartDate, reportInterval)
      breakable {
        while (true) {
          implicit val timeOut: Timeout = Timeout(5 seconds)
          (Await.result(scheduler ? CheckState, Duration.Inf)).asInstanceOf[SchedulerState] match {
            case Idle =>
              scheduler ! PoisonPill
              Thread.sleep(5000)
              break
            case any =>
              workerLog.info(s"CheckState is $any")
              Thread.sleep(5000)
          }
        }
      }

    }
  }


  process
  exit()


}
