package edu.uci.ics.cloudberry.zion.experiment

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Props}
import edu.uci.ics.cloudberry.zion.experiment.Common.Reporter.{Fin, OneShot}
import edu.uci.ics.cloudberry.zion.experiment.ResponseTime.{estimateInGeneral, getGroupByDateAndStateAQL}
import org.joda.time.DateTime
import play.api.libs.json.JsArray

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration


object Control extends App with Connection {
  import Common._

  def process: Unit = {
    val terminate = urStartDate
    //        0.9 to 0.1 by -0.1 foreach { discount =>
    //      45 to 2600 by 5 foreach { minGap=>
    //      2 to 3 foreach { runs =>
    2 to 3 foreach { runs =>
      Seq(AlgoType.Baseline,AlgoType.NormalGaussian,AlgoType.Histogram).foreach { algo =>
//      Seq(AlgoType.Baseline).foreach { algo =>
        val alpha = Math.pow(2, runs - 1)
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
          val list = streamRun(requireTime, algo, alpha, reporter, keyword, history, fullHistory, urEndDate, 2, requireTime, requireTime)
//          val list = cancelableScheduling(requireTime, urEndDate, Parameters(requireTime, algo, alpha, reporter, keyword, 1), State(history, fullHistory))
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
    val aql = getGroupByDateAndStateAQL(start, range, if (keyword.length > 0) Some(keyword) else None)
    val (runTime, _, count) = multipleTime(0, aql)
    reporter ! OneShot(start, range, count, JsArray(Seq.empty))

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


  process
  exit()
}
