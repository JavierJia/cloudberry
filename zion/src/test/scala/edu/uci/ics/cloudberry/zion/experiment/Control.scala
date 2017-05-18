package edu.uci.ics.cloudberry.zion.experiment

import java.util.concurrent.TimeUnit

import akka.actor.Status.Success
import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import edu.uci.ics.cloudberry.zion.experiment.Common.QueryStat
import edu.uci.ics.cloudberry.zion.experiment.ResponseTime.{AlgoType, estimateInGeneral, getAQL}
import org.joda.time.{DateTime, Interval}
import play.api.Logger
import play.api.libs.ws.WSResponse

import scala.collection.mutable
import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration.FiniteDuration


object Control extends App with Connection {
  val reportLog = Logger("report")
  val workerLog = Logger("worker")

  class Reporter(keyword:String, limit: FiniteDuration) extends Actor {

    object Report

    var schedule = context.system.scheduler.schedule(limit, limit, self, Report)
    val startTime = DateTime.now();

    val queue: mutable.Queue[OneShot] = new mutable.Queue[OneShot]()

    var numReports = 0
    var numFailed = 0
    var delayed = 0l
    var sumResult = 0

    def hungry(since: DateTime): Actor.Receive = {
      case r: OneShot =>
        val delay = new Interval(since, DateTime.now())
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
          reportLog.info(s"$keyword, report all rest results")
          numReports += 1
          sumResult += queue.dequeueAll(r => true).map(_.count).sum
        }
        val totalTime = DateTime.now().getMillis - startTime.getMillis
        reportLog.info(s"$keyword numOfReports: $numReports, sumTime: ${totalTime / 1000.0}, numOfFail: $numFailed, sumDelay: ${delayed / 1000.0}, sumCount:$sumResult")
        reportLog.info(s"=============FIN===============")
        self ! PoisonPill
      }
    }
  }

  case class OneShot(start: DateTime, range: Int, count:Int)

  case object Fin

  def process: Unit = {
    val terminate = urStartDate
    //        0.9 to 0.1 by -0.1 foreach { discount =>
    //      45 to 2600 by 5 foreach { minGap=>
    //      2 to 3 foreach { runs =>
    1 to 1 foreach { runs =>
//      Seq(AlgoType.Baseline,AlgoType.NormalGaussian,AlgoType.Histogram).foreach { algo =>
      Seq(AlgoType.Baseline).foreach { algo =>
        //        val alpha = Math.pow(2, runs - 1)
        val alpha = 1
        val requireTime = 2000
        val fullHistory = List.newBuilder[QueryStat]

        for (keyword <- keywords) {
          val history = List.newBuilder[QueryStat]
          fullHistory.clear() //FIXME !!!!!
          val weight = List.newBuilder[Long]
          weight += 1
          weight += 1


          val tick = DateTime.now()

          val reporter = system.actorOf(Props(new Reporter(keyword, FiniteDuration(requireTime, TimeUnit.MILLISECONDS))))
//          val list = streamRun(requireTime, algo, alpha, reporter, keyword, history, fullHistory, urEndDate, 2, requireTime, requireTime)
          val list = cancelableScheduling(requireTime, urEndDate, Parameters(requireTime, algo, alpha, reporter, keyword, 1), State(history, fullHistory))
            .takeWhile(_.getMillis > terminate.getMillis).toList
          val tock = DateTime.now()
          val totalTime = tock.getMillis - tick.getMillis
          reporter ! Fin
          //          val penalty = list.filter(_._2 < 0).map(_._2).map(x => Math.ceil(-x.toDouble / requireTime)).sum
          //          val sumPenalty = list.filter(_._2 < 0).map(_._2).sum
          //          val (xa, xb) = linearInterpolation(history.result(), history.result().size)
          val histo = history.result()
          val variance = histo.map(h => (h.targetMS - h.actualMS) * (h.targetMS - h.actualMS)).sum.toDouble / histo.size
          workerLog.info("algorithm,alpha,requireTime,unit,keyword,numQuery,totalTime,aveTime, $variance")
          workerLog.info(s"$algo,$alpha,$requireTime,$unit,$keyword,${list.size},${totalTime / 1000.0},${totalTime / list.size / 1000.0}, $variance")
        }
      }
    }
  }

  def streamRun(deadline: Int,
                algo: AlgoType.Value,
                alpha: Double,
                reporter: ActorRef,
                keyword: String,
                history: mutable.Builder[Common.QueryStat, List[Common.QueryStat]],
                fullHistory: mutable.Builder[Common.QueryStat, List[Common.QueryStat]],
                endTime: DateTime, range: Int, limit: Int, target: Int): Stream[DateTime] = {
    val start = endTime.minusHours(range)
    val aql = getAQL(start, range, if (keyword.length > 0) Some(keyword) else None)
    val (runTime, _, count) = multipleTime(0, aql)
    reporter ! OneShot(start, range, count)

    history += QueryStat(target, range, runTime.toInt)
    fullHistory += QueryStat(target, range, runTime.toInt)
    //    weight += Math.min(weight.result().takeRight(2).sum, Integer.MAX_VALUE)
    workerLog.info(s"$algo,$alpha,$start,$range,$keyword,$limit,$target,$runTime,$count")

    //            val diff = if (runTime <= limit) limit - runTime else -((runTime - limit) % requireTime)
    val diff = if (runTime <= limit) limit - runTime else 0
    val nextLimit = deadline + diff.toInt

    val (nextRange, estTarget) = estimateInGeneral(nextLimit, alpha, history.result(), fullHistory.result(), algo)

    endTime #:: streamRun(deadline, algo, alpha,
      reporter, keyword, history, fullHistory, start, nextRange.toInt, nextLimit, estTarget.toInt)
  }

  def runAQuery(query: String, contextID:Int ): Future[ResultFromDB] = {
    val start = DateTime.now
    adbConn.postQuery(query, Some(contextID)).map { ret =>
      val duration = DateTime.now.getMillis - start.getMillis
      val sum = (ret \\ "count").map(_.as[Int]).sum
      ResultFromDB(duration, sum)
    }
  }

  def cancelPreviousQuery(contextID: Int): Unit = {
    adbConn.cancel(cancelURL, contextID).map { response: WSResponse =>
      Logger.info(response.body)
    }
  }

  case class ResultFromDB(mills: Long, sum: Int)

  case class Parameters(reportInterval: Int,
                        algo: AlgoType.Value,
                        alpha: Double,
                        reporter: ActorRef,
                        keyword: String,
                        minHours: Int)

  case class State(history: mutable.Builder[Common.QueryStat, List[Common.QueryStat]],
                   fullHistory: mutable.Builder[Common.QueryStat, List[Common.QueryStat]])

  def learnQueryState(start: DateTime, range: Int, estimate: Option[Long], actual: Long, state: State) = {
    state.history += QueryStat(estimate.getOrElse(actual).toInt, range, actual.toInt)
    state.fullHistory += QueryStat(estimate.getOrElse(actual).toInt, range, actual.toInt)
  }

  var idAdventrue = 2
  def getIDADV: Int = {
    val ret = idAdventrue
    idAdventrue+=1
    ret
  }

  def decideMinWindow(parameters: Parameters, state: State): Int = {
    //option one, pick the most recent min time
    val history = state.history.result()
    val id = history.lastIndexWhere(_.estSlice == parameters.minHours)
    if (id > 0){
      history(id).actualMS
    } else {
      0.5
    }
  }

  def cancelableOneOnOne(curDeadLine: Int,
                         endTime: DateTime,
                         parameters: Parameters,
                         state: State): Stream[DateTime]= {
    val makeupWindow: Int = decideMinWindow(parameters, state)
    val preDeadline = curDeadLine - makeupWindow

    if (preDeadline < makeupWindow){
      ???
    }

    val optKeyword = if (parameters.keyword.length > 0) Some(parameters.keyword) else None

    val (adventureRange, estTarget) = estimateInGeneral(preDeadline, parameters.alpha, state.history.result(), state.fullHistory.result(), parameters.algo)
    val startTime = endTime.minusHours(adventureRange.toInt)
    val queryAdventure = getAQL(startTime, adventureRange.toInt, optKeyword)

    val f = runAQuery(queryAdventure, getIDADV)

    try {
      val ret = Await.result(f, FiniteDuration.apply(preDeadline, TimeUnit.MILLISECONDS))
      parameters.reporter ! OneShot(startTime, parameters.minHours, ret.sum)

      val extra = curDeadLine - ret.mills + parameters.reportInterval
      learnQueryState(startTime, adventureRange.toInt, Some(estTarget.toLong), ret.mills, state)
      return endTime #:: cancelableScheduling(extra.toInt, startTime, parameters, state)
    } catch {
      case e: TimeoutException => {
        val startMakeUp = endTime.minusHours(parameters.minHours)
        val makeupQuery = getAQL(startMakeUp, parameters.minHours, optKeyword)
        val queryIDMakeup = getIDADV
        val fMakeup = runAQuery(makeupQuery, queryIDMakeup)

        //wait for whichever comes first
        ???
        val retMakeup = Await.result(fMakeup, scala.concurrent.duration.Duration.Inf)
        parameters.reporter ! OneShot(startMakeUp, parameters.minHours, retMakeup.sum)

        learnQueryState(startMakeUp, parameters.minHours, Some(retMin.mills), retMakeup.mills, state)

        workerLog.info(s"Makeupqr: ${parameters.algo},${parameters.alpha},$startMakeUp,${parameters.minHours},${parameters.keyword},${retMin.mills},${retMin.mills},${retMakeup.mills},${retMakeup.sum}")

        val residual = Math.max(0, retMin.mills - retMakeup.mills).toInt

        return endTime #:: cancelableScheduling(residual + parameters.reportInterval, startMakeUp, parameters, state)
    }
  }

  def cancelableScheduling(curDeadline: Int,
                           endTime: DateTime,
                           parameters: Parameters,
                           state: State
                          ): Stream[DateTime] = {

    val QueryIDNormal = 1
    val QueryIDADV = getIDADV
    val start = endTime.minusHours(parameters.minHours)
    // first minQuery
    val optKeyword = if (parameters.keyword.length > 0) Some(parameters.keyword) else None
    val query = getAQL(start, parameters.minHours, optKeyword)
    val f = runAQuery(query, QueryIDNormal)
    // we have to wait no matter how slow it it
    val retMin = Await.result(f, scala.concurrent.duration.Duration.Inf)
    parameters.reporter ! OneShot(start, parameters.minHours, retMin.sum)

    workerLog.info(s"FirstMin: ${parameters.algo},${parameters.alpha},$start,${parameters.minHours},${parameters.keyword},${curDeadline},MIN,${retMin.mills},${retMin.sum}")
    learnQueryState(start, parameters.minHours, None, retMin.mills, state)

    // no room for adventure, start over again
    if (retMin.mills * 3 > curDeadline + parameters.reportInterval) {
      if (retMin.mills > curDeadline) {
        workerLog.info(s"min query longer than current deadline. deadline: $curDeadline, actual: ${retMin.mills}")
        return endTime #:: cancelableScheduling(parameters.reportInterval, start, parameters, state)
      } else {
        val extraTime = curDeadline - retMin.mills.toInt
        workerLog.info(s"min query too long to take adventure. deadline: $curDeadline, actual: ${retMin.mills}, extra: $extraTime")
        return endTime #:: cancelableScheduling(extraTime + parameters.reportInterval, start, parameters, state)
      }
    }

    // take adventure
    val target = curDeadline + parameters.reportInterval - 2 * retMin.mills
    val (adventureRange, estTarget) = estimateInGeneral(target.toInt, parameters.alpha, state.history.result(), state.fullHistory.result(), parameters.algo)
    val startAdventure = start.minusHours(adventureRange.toInt)
    val queryAdventure = getAQL(startAdventure, adventureRange.toInt, optKeyword)

    val fAdv = runAQuery(queryAdventure, QueryIDADV)
    try {
      val retAdv = Await.result(fAdv, FiniteDuration(target, TimeUnit.MILLISECONDS))
      parameters.reporter ! OneShot(startAdventure, adventureRange.toInt, retAdv.sum)

      workerLog.info(s"Advnture: ${parameters.algo},${parameters.alpha},$startAdventure,${adventureRange.toInt},${parameters.keyword},${target},${estTarget.toInt},${retAdv.mills},${retAdv.sum}")

      val residual = curDeadline + parameters.reportInterval - retMin.mills - retAdv.mills
      learnQueryState(startAdventure, adventureRange.toInt, Some(estTarget.toLong), retAdv.mills, state)
      return endTime #:: start #:: cancelableScheduling(residual.toInt, startAdventure, parameters, state)
    } catch {
      case e: TimeoutException =>
        // cancel it and start over again
        cancelPreviousQuery(QueryIDADV)
        workerLog.info(s"Cancel Adventure query:$startAdventure,${adventureRange.toInt}")
        val startMakeUp = start.minusHours(parameters.minHours)
        val makeupQuery = getAQL(startMakeUp, parameters.minHours, optKeyword)
        val fMakeup = runAQuery(makeupQuery, QueryIDNormal)
        // we have to wait no matter how slow it it
        val retMakeup = Await.result(fMakeup, scala.concurrent.duration.Duration.Inf)
        parameters.reporter ! OneShot(startMakeUp, parameters.minHours, retMakeup.sum)

        learnQueryState(startMakeUp, parameters.minHours, Some(retMin.mills), retMakeup.mills, state)

        workerLog.info(s"Makeupqr: ${parameters.algo},${parameters.alpha},$startMakeUp,${parameters.minHours},${parameters.keyword},${retMin.mills},${retMin.mills},${retMakeup.mills},${retMakeup.sum}")

        val residual = Math.max(0, retMin.mills - retMakeup.mills).toInt

        return endTime #:: cancelableScheduling(residual + parameters.reportInterval, startMakeUp, parameters, state)
    }
  }

  process
  exit()
}