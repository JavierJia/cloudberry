package edu.uci.ics.cloudberry.zion.experiment

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import edu.uci.ics.cloudberry.zion.experiment.Common.QueryStat
import edu.uci.ics.cloudberry.zion.experiment.ResponseTime.{AlgoType, estimateInGeneral, getAQL}
import org.joda.time.DateTime
import play.api.Logger

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration


object Control extends App with Connection {
  val reportLog = Logger("reporter")
  val workerLog = Logger("worker")

  class Reporter(limit: FiniteDuration) extends Actor {

    object Report

    context.system.scheduler.schedule(limit, limit, self, Report)

    val queue: mutable.Queue[Result] = new mutable.Queue[Result]()

    var numReports = 0
    var numFailed = 0

    override def receive = {
      case result: Result => queue.enqueue(result)
      case Report => {
        if (queue.isEmpty) {
          reportLog.info(s"!!!no result")
          numFailed += 1
        } else {
          val result = queue.dequeue()
          reportLog.info(s"report result from ${result.start} of range ${result.range}")
        }
        numReports += 1
      }
      case Fin => {
        if (!queue.isEmpty) {
          reportLog.info(s"report all rest results")
          numReports += 1
        }
        reportLog.info(s"numOfReports: $numReports, numOfFails: $numFailed")
        self ! PoisonPill
      }
    }
  }

  case class Result(start: DateTime, range: Int)

  case object Fin

  def process: Unit = {
    val terminate = urStartDate
    //        0.9 to 0.1 by -0.1 foreach { discount =>
    //      45 to 2600 by 5 foreach { minGap=>
    //      2 to 3 foreach { runs =>
    1 to 1 foreach { runs =>
      Seq(AlgoType.Histogram, AlgoType.NormalGaussian, AlgoType.Baseline).foreach { algo =>
//        val alpha = Math.pow(2, runs - 1)
        val alpha = 1
        val requireTime = 2000
        val fullHistory = List.newBuilder[QueryStat]

        for (keyword <- keywords) {
          val history = List.newBuilder[QueryStat]
          val weight = List.newBuilder[Long]
          weight += 1
          weight += 1

          def streamRun(reporter: ActorRef, endTime: DateTime, range: Int, limit: Int, target: Int): Stream[(DateTime, Int)] = {
            val start = endTime.minusHours(range)
            val aql = getAQL(start, range, if (keyword.length > 0) Some(keyword) else None)
            val (runTime, _, count) = multipleTime(0, aql)
            reporter ! Result(start, range)

            history += QueryStat(target, range, runTime.toInt)
            fullHistory += QueryStat(target, range, runTime.toInt)
            weight += Math.min(weight.result().takeRight(2).sum, Integer.MAX_VALUE)
            workerLog.info(s"$algo,$alpha,$start,$range,$keyword,$limit,$target,$runTime,$count")

            //            val diff = if (runTime <= limit) limit - runTime else -((runTime - limit) % requireTime)
            val diff = if (runTime <= limit) limit - runTime else 0
            val nextLimit = requireTime + diff.toInt

            val (nextRange, estTarget) = estimateInGeneral(nextLimit, alpha, history.result(), fullHistory.result(), algo)

            (endTime, limit - runTime.toInt) #:: streamRun(reporter, start, nextRange.toInt, nextLimit, estTarget.toInt)
          }

          val tick = DateTime.now()

          val reporter = system.actorOf(Props(new Reporter(FiniteDuration(requireTime, TimeUnit.MILLISECONDS))))
          val list = streamRun(reporter, urEndDate, 2, requireTime, requireTime).takeWhile(_._1.getMillis > terminate.getMillis).toList
          val tock = DateTime.now()
          val totalTime = tock.getMillis - tick.getMillis
          reporter ! Fin
          val penalty = list.filter(_._2 < 0).map(_._2).map(x => Math.ceil(-x.toDouble / requireTime)).sum
          val sumPenalty = list.filter(_._2 < 0).map(_._2).sum
          //          val (xa, xb) = linearInterpolation(history.result(), history.result().size)
          val histo = history.result()
          val variance = histo.map(h => (h.targetMS - h.actualMS) * (h.targetMS - h.actualMS)).sum.toDouble / histo.size
          workerLog.info("algorithm,alpha,requireTime,unit,keyword,numQuery,numPenalty,sumPenalty,totalTime,aveTime, $variance")
          workerLog.info(s"$algo,$alpha,$requireTime,$unit,$keyword,${list.size},${penalty},${sumPenalty / 1000.0},${totalTime / 1000.0},${totalTime / list.size / 1000.0}, $variance")
          workerLog.info("===================")
        }
      }
    }
  }

  process
}
