package edu.uci.ics.cloudberry.zion.experiment

import java.util.concurrent.TimeUnit

import edu.uci.ics.cloudberry.zion.experiment.Common.Reporter.OneShot
import edu.uci.ics.cloudberry.zion.experiment.Common._
import org.joda.time.DateTime
import play.api.Logger
import play.api.libs.ws.WSResponse

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future, TimeoutException}

object CancelableControl extends App with Connection {

  val reportLog = Logger("report")
  val workerLog = Logger("worker")

  var idAdventrue = 2

  def getIDADV: Int = {
    val ret = idAdventrue
    idAdventrue += 1
    ret
  }

  def runAQuery(query: String, contextID: Int): Future[ResultFromDB] = {
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



  def cancelableScheduling(curDeadline: Int,
                           endTime: DateTime,
                           parameters: Parameters,
                           state: HistoryStats
                          ): Stream[DateTime] = {

    val QueryIDNormal = 1
    val QueryIDADV = getIDADV
    val start = endTime.minusHours(parameters.minHours)
    // first minQuery
    val optKeyword = if (parameters.keyword.length > 0) Some(parameters.keyword) else None
    val query = ResponseTime.getAQL(start, parameters.minHours, optKeyword)
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
    val (adventureRange, estTarget) = ResponseTime.estimateInGeneral(target.toInt, parameters.alpha, state.history.result(), state.fullHistory.result(), parameters.algo)
    val startAdventure = start.minusHours(adventureRange.toInt)
    val queryAdventure = ResponseTime.getAQL(startAdventure, adventureRange.toInt, optKeyword)

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
        val makeupQuery = ResponseTime.getAQL(startMakeUp, parameters.minHours, optKeyword)
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

}

