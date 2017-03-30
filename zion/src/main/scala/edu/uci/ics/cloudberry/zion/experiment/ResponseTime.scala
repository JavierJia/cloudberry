package edu.uci.ics.cloudberry.zion.experiment

import java.io.File
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import edu.uci.ics.cloudberry.zion.experiment.Stats.obs
import edu.uci.ics.cloudberry.zion.model.datastore.AsterixConn
import edu.uci.ics.cloudberry.zion.model.impl.{AQLGenerator, TwitterDataStore}
import edu.uci.ics.cloudberry.zion.model.schema._
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoint, WeightedObservedPoints}
import org.apache.commons.math3.stat.Frequency
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.asynchttpclient.AsyncHttpClientConfig
import org.joda.time.{DateTime, Duration}
import play.api.libs.ws.WSConfigParser
import play.api.libs.ws.ahc.{AhcConfigBuilder, AhcWSClient, AhcWSClientConfig}
import play.api.{Configuration, Environment, Mode}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.{Await, ExecutionContext, Future}

trait Connection {

  case class QueryStat(targetMS: Int, estSlice: Int, actualMS: Int)

  implicit val cmp: Ordering[DateTime] = Ordering.by(_.getMillis)
  val urStartDate = new DateTime(2016, 11, 4, 15, 47)
  val urEndDate = new DateTime(2017, 1, 17, 6, 16)
  val numberLSMs = 262
  val unit = new Duration(urStartDate, urEndDate).dividedBy(numberLSMs).getStandardHours

  val keywords = Seq("zika", "flood", "election", "clinton", "trump", "happy", "")

  val gen = new AQLGenerator()
  val aggrCount = AggregateStatement(AllField, Count, NumberField("count"))
  val globalAggr = GlobalAggregateStatement(aggrCount)

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  val wsClient = produceWSClient()
  val url = "http://uranium.ics.uci.edu:19002/aql"
  //  val url = "http://localhost:19002/aql"
  val asterixConn = new AsterixConn(url, wsClient)

  def produceWSClient(): AhcWSClient = {
    val configuration = Configuration.reference ++ Configuration(ConfigFactory.parseString(
      """
        |play.ws.followRedirects = true
        |play {
        |  ws {
        |    timeout {
        |      connection = 600000
        |      idle = 6000000
        |    }
        |  }
        |}
      """.stripMargin))

    // If running in Play, environment should be injected
    val environment = Environment(new File("."), this.getClass.getClassLoader, Mode.Prod)

    val parser = new WSConfigParser(configuration, environment)
    val config = AhcWSClientConfig(wsClientConfig = parser.parse())
    val builder = new AhcConfigBuilder(config)
    val logging = new AsyncHttpClientConfig.AdditionalChannelInitializer() {
      override def initChannel(channel: io.netty.channel.Channel): Unit = {
        //        channel.pipeline.addFirst("log", new io.netty.handler.logging.LoggingHandler("info", LogLevel.ERROR))
      }
    }
    val ahcBuilder = builder.configure()
    ahcBuilder.setHttpAdditionalChannelInitializer(logging)
    val ahcConfig = ahcBuilder.build()
    new AhcWSClient(ahcConfig)
  }

  def exit(): Unit = {
    wsClient.close()
    System.exit(0)
  }

  def multipleTime(times: Int, aql: String): (Long, Double, Int) = {
    var sum = 0l
    var count = 0
    var firstTime = 0l
    0 to times foreach { i =>
      val (time, c) = timeQuery(aql, "count")
      if (i == 0) {
        firstTime = time
        count = c
      } else {
        sum += time
        count = c
      }
    }
    (firstTime, sum * 1.0 / (times + Double.MinPositiveValue), count)
  }

  def timeQuery(aql: String, field: String): (Long, Int) = {
    val start = DateTime.now()
    val f = asterixConn.postQuery(aql).map { ret =>
      val duration = new Duration(start, DateTime.now())
      val sum = (ret \\ field).map(_.as[Int]).sum
      //      println("time: " + duration.getMillis + " " + field + ": " + value)
      (duration.getMillis, sum)
    }
    Await.result(f, scala.concurrent.duration.Duration.Inf)
  }

  def timeQuery(aqls: Seq[String], field: String, inParallel: Boolean): (Long, Seq[Int]) = {
    val start = DateTime.now()
    if (inParallel) {
      val f = Future.traverse(aqls) { aql =>
        asterixConn.postQuery(aql).map { ret => (ret \\ field).map(_.as[Int]).sum }
      }
      val counts = Await.result(f, scala.concurrent.duration.Duration.Inf)
      (DateTime.now.getMillis - start.getMillis, counts)
    } else {
      val counts = Seq.newBuilder[Int]
      aqls.foreach { aql =>
        val f = asterixConn.postQuery(aql).map(ret => (ret \\ field).map(_.as[Int]).sum)
        val c = Await.result(f, scala.concurrent.duration.Duration.Inf)
        counts += c
      }
      (DateTime.now.getMillis - start.getMillis, counts.result())
    }
  }

}

object ResponseTime extends App with Connection {

  //  TestExternalSort(gen)
  //  testID
  //  exit()

  //  warmUp()
  //  searchBreakdown(gen)
  //  startToEnd()
  testAdaptiveShot()


  def warmUp(): Unit = {
    val aql =
      """
        |for $d in dataset twitter.ds_tweet
        |where $d.create_at >= datetime('2016-11-01T08:00:00.000Z') and
        |      $d.create_at <= datetime('2016-11-10T08:00:00.000Z')
        |group by $g := get-day($d.create_at) with $d
        |return { "day": $g, "count": count($d)}
      """.stripMargin
    val f = asterixConn.postQuery(aql)
    Await.result(f, scala.concurrent.duration.Duration.Inf)
  }

  def clearCache(): Unit = {
    val aql =
      """
        |count(for $d in dataset twitter.ds_tweet
        |where $d.create_at >= datetime('2016-07-01T08:00:00.000Z') and
        |      $d.create_at <= datetime('2016-08-30T08:00:00.000Z')
        |return $d)
      """.stripMargin
    val f = asterixConn.postQuery(aql)
    Await.result(f, scala.concurrent.duration.Duration.Inf)
  }

  def startToEnd(): Unit = {
    //    for (keyword <- keywords) {
    for (keyword <- Seq("zika")) {
      val aql = getAQL(urStartDate, new Duration(urStartDate, urEndDate).getStandardHours.toInt, Some(keyword))
      val (runTime, avgTime, count) = multipleTime(0, aql)
      println(s"$urStartDate,$urEndDate,$keyword, cold:$runTime, warm: $avgTime, $count")
    }
  }

  def testAdaptiveShot(): Unit = {
    val gaps = Seq(1, 2, 4, 8, 16, 32, 64, 128)
    //    val keywords = Seq("happy", "zika", "uci", "trump", "a")
    //    val keywords = Seq("zika", "pitbull", "goal", "bro", "happy")
    //    keywordWithTime()
    //    selectivity(keywords)
    //      keywordWithContinueTime()
    elasticTimeGap()
    //    minimalTimeGap()
    //    testOverheadOfMultipleQueries()
    //    testSamplingPerf()
    //    elasticAdaptiveGap()


    def selectivity(seq: Seq[Any]): Unit = {
      for (s <- seq) {
        val aql = {
          s match {
            case x: Int =>
              getCountTime(DateTime.now().minusHours(x), DateTime.now())
            case string: String =>
              getCountKeyword(string)
            case _ =>
              throw new IllegalArgumentException()
          }
        }
        val (firstTime, avg, count) = multipleTime(5, aql)
        println(s"firstTime\t$firstTime")
        println(s"avgTime\t$avg")
        println(s"count\t$count")
      }
    }

    def keywordWithTime(): Unit = {
      for (gap <- gaps) {
        for (keyword <- keywords) {
          clearCache()
          val now = DateTime.now()
          val aql = getAQL(now.minusHours(gap), gap, Some(keyword))

          val (firstTime, avg, count) = multipleTime(0, aql)
          println(s"gap:$gap\tkeyword:$keyword")
          println(s"firstTime\t$firstTime")
          println(s"avgTime\t$avg")
          println(s"count\t$count")
        }
      }
    }

    def keywordWithContinueTime(): Unit = {
      val repeat = 15
      for (gap <- gaps) {
        for (keyword <- keywords) {
          var start = DateTime.now()
          1 to repeat foreach { i =>

            val aql = getAQL(start.minusHours(gap), gap, Some(keyword))
            val (firstTime, avg, count) = multipleTime(0, aql)
            println(
              s"""
                 |gap,keyword,time,count
                 |$gap,$keyword,$firstTime,$count
               """.stripMargin.trim)
            start = start.minusHours(gap)
          }
        }
      }
    }

    def linearInterpolateUsingCommon(history: List[QueryStat]): Stats.Coeff = {
      val obs: WeightedObservedPoints = new WeightedObservedPoints()
      history.foreach(h => obs.add(h.estSlice, h.actualMS))
      Stats.linearFitting(obs)
    }

    // a + bx
    def linearInterpolation(history: List[QueryStat], totalNumber: Int): (Double, Double) = {
      val (a2, b2, ab, a, b) = history.takeRight(totalNumber).foldRight((0l, 0l, 0l, 0l, 0l)) {
        case (stats: QueryStat, (lastA2, lastB2, lastAB, lastA, lastB)) =>
          (
            lastA2 + 1,
            lastB2 + stats.estSlice * stats.estSlice,
            lastAB + 2 * stats.estSlice,
            lastA + 2 * stats.actualMS,
            lastB + 2 * stats.estSlice * stats.actualMS
          )
      }
      //      b2 += range * range
      //      a2 += 1
      //      ab += 2 * range
      //      a += 2 * runTime.toInt
      //      b += 2 * range * runTime.toInt
      val divider = 2 * b2 - ab * ab / (2.0 * a2)

      val xbRaw = (b - a * ab / (2.0 * a2)) / (if (divider == 0) Double.MinPositiveValue else divider)
      val xb = Math.max(xbRaw, Double.MinPositiveValue)
      val xa = (b - 2.0 * b2 * xb) / ab
      (xa, xb)
    }

    def distance(xa: Double, xb: Double, x: Double, y: Double): Double = {
      val thY = xb * x + xa
      val thX = (y - xa) / xb
      val lineA = Math.abs(x - thX)
      val lineB = Math.abs(y - thY)
      val lineC = Math.sqrt(lineA * lineA + lineB * lineB)
      lineA * lineB / lineC
    }

    def minimalTimeGap(): Unit = {
      val terminate = urStartDate
      //      1 to 25 by 2 foreach { i =>
      Seq(1, 3000) foreach { i =>
        //      1 to 4 foreach { i =>
        //        val requireTime = 500 * i
        val requireTime = 2000
        for (keyword <- keywords) {
          var (b2, a2, ab, a, b) = (0l, 0l, 0l, 0l, 0l)

          def streamRun(endTime: DateTime, range: Int, target: Int): Stream[(DateTime, Int)] = {
            val start = endTime.minusHours(range)
            val aql = getAQL(start, range, if (keyword.length > 0) Some(keyword) else None)
            val (runTime, _, count) = multipleTime(0, aql)

            //            println(s"$start,$range,$keyword,$target,$runTime,$count")
            b2 += range * range
            a2 += 1
            ab += 2 * range
            a += 2 * runTime.toInt
            b += 2 * range * runTime.toInt

            val nextTarget = requireTime + Math.max(target - runTime.toInt, 0)
            //            val nextRange = Math.max(formular(nextTarget, range, runTime, b2, a2, ab, b, a, lambda = 1), 1)
            val nextRange = i

            (endTime, target - runTime.toInt) #:: streamRun(start, nextRange, nextTarget)
          }

          val tick = DateTime.now()

          val list = streamRun(urEndDate, 2, requireTime).takeWhile(_._1.getMillis > terminate.getMillis).toList
          // run an entire query
          // val list = streamRun(DateTime.now(), new Duration(terminate, DateTime.now()).getStandardHours.toInt * 2, requireTime).takeWhile(_._1.getMillis > terminate.getMillis).toList
          val tock = DateTime.now()
          val totalTime = tock.getMillis - tick.getMillis
          val penalty = list.count(_._2 < 0)
          val sumPenalty = list.filter(_._2 < 0).map(_._2).sum
          println(s"$i,$requireTime,$keyword,${list.size},${penalty},${sumPenalty},${totalTime},${totalTime / list.size}")
          //          val (xb, xa) = linearInterpolation(b2, a2, ab, b, a)
          //          println(s"interpolation: b = $xb, a = $xa")
        }
      }
    }


    /** entry point */
    def elasticTimeGap(): Unit = {

      val terminate = urStartDate
      //        0.9 to 0.1 by -0.1 foreach { discount =>
      //      45 to 2600 by 5 foreach { minGap=>
      val fullHistory = List.newBuilder[QueryStat]
      2 to 3 foreach { runs =>
        val alpha = runs
        val requireTime = 2000
        for (keyword <- keywords) {
          val history = List.newBuilder[QueryStat]

          def streamRun(endTime: DateTime, range: Int, limit: Int, target: Int): Stream[(DateTime, Int)] = {
            val start = endTime.minusHours(range)
            val aql = getAQL(start, range, if (keyword.length > 0) Some(keyword) else None)
            val (runTime, _, count) = multipleTime(0, aql)

            history += QueryStat(target, range, runTime.toInt)
            fullHistory += QueryStat(target, range, runTime.toInt)
            println(s"$start,$range,$keyword,$limit,$target,$runTime,$count")

            //            val diff = if (runTime <= limit) limit - runTime else -((runTime - limit) % requireTime)
            val diff = if (runTime <= limit) limit - runTime else 0
            val nextLimit = requireTime + diff.toInt

            //            val estTarget = estimateTarget(nextLimit, alpha, history.result())
            //            val estTarget = estimateTargetUsing3function(nextLimit, alpha, history.result())
            //            val estTarget = estimateTargetUsingHisto(nextLimit, alpha, if (runs == 1) history.result() else fullHistory.result())
//            val estTarget = estimateTargetUsingHisto(nextLimit, alpha, history.result())
            val estTarget = estimateTargetUsingHisto(nextLimit, alpha, fullHistory.result())
            val nextRangeInHour = Math.max(formularUsingStatsLib(estTarget, history.result()), 1)
            val nextRange = (Math.ceil(nextRangeInHour / unit.toDouble) * unit).toInt

            (endTime, limit - runTime.toInt) #:: streamRun(start, nextRange, nextLimit, estTarget)
          }

          val tick = DateTime.now()

          val list = streamRun(urEndDate, 2, requireTime, requireTime).takeWhile(_._1.getMillis > terminate.getMillis).toList
          val tock = DateTime.now()
          val totalTime = tock.getMillis - tick.getMillis
          val penalty = list.filter(_._2 < 0).map(_._2).map(x => Math.ceil(-x.toDouble / requireTime)).sum
          val sumPenalty = list.filter(_._2 < 0).map(_._2).sum
          //          val (xa, xb) = linearInterpolation(history.result(), history.result().size)
          val histo = history.result()
          val variance = histo.map(h => (h.targetMS - h.actualMS) * (h.targetMS - h.actualMS)).sum.toDouble / histo.size
          println(s"$requireTime,$unit,$keyword,${list.size},${penalty},${sumPenalty},${totalTime},${totalTime / list.size}, $variance")
        }
      }
    }

    def testSamplingPerf(): Unit = {
      val end = DateTime.now()
      val start = end.minusDays(90)
      val verticalMinutesGap = 30

      def genHorizontalPair(horizontalHourSlice: Int, trackBackMinutes: Int): Stream[(DateTime, DateTime)] = {
        def s: Stream[DateTime] = end #:: s.map(_.minusHours(horizontalHourSlice))

        s.takeWhile(_.getMillis > start.getMillis).map { endTime =>
          val startTime = new DateTime(Math.max(endTime.minusHours(horizontalHourSlice).getMillis, start.getMillis))
          val startMinutes = new DateTime(Math.max(
            endTime.minusMinutes(trackBackMinutes).minusMinutes(verticalMinutesGap).getMillis, startTime.getMillis))
          (startMinutes, endTime.minusMinutes(trackBackMinutes))
        }.takeWhile(p => p._1.getMillis < p._2.getMillis)
      }

      val hSlice = 24
      val inParallel = false
      for (keyword <- keywords) {
        val tick = DateTime.now()
        1 to hSlice * 60 / verticalMinutesGap foreach { i =>
          val (runTime, results) = timeQuery(getSeqTimeAQL(genHorizontalPair(hSlice, (i - 1) * verticalMinutesGap), keyword), "count", inParallel)
          println(s"$i,$keyword,$runTime,${results.sum}")
        }
        println(s"$keyword,${DateTime.now().getMillis - tick.getMillis}")
      }
    }

    def testOverheadOfMultipleQueries(): Unit = {
      val end = new DateTime(2016, 10, 6, 0, 0)
      val start = new DateTime(2016, 7, 1, 0, 0)

      def genPair(gapHour: Int): Stream[(DateTime, Int)] = {
        def s: Stream[DateTime] = end #:: s.map(_.minusHours(gapHour))

        s.takeWhile(_.getMillis > start.getMillis).map { endTime =>
          val startTime = new DateTime(Math.max(endTime.minusHours(gapHour).getMillis, start.getMillis))
          (startTime, new Duration(startTime, endTime).getStandardHours.toInt)
        }
      }

      Seq(1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024).foreach { slices =>
        val gap = new Duration(start, end).dividedBy(slices).getStandardHours.toInt
        for (keyword <- keywords) {
          val begin = DateTime.now()
          genPair(gap).foreach { case (startTime, g) =>
            val aql = getAQL(startTime, g, Some(keyword))
            val (lastTime, _, count) = multipleTime(0, aql)
            println(s"$keyword,$startTime,$g,$lastTime,$count")
          }
          println(s"$keyword,$slices,${DateTime.now().getMillis - begin.getMillis}")
          println()
        }
      }
    }

    def estimateTargetUseGassianModel(limit: Int, alpha: Double, history: List[QueryStat]): Int = {
      // 1. use percent discount
      // 2. use abs difference
      // 3. find the median of the values
      if (history.size < 3) {
        return 1
      }
      val variance = history.takeRight(history.size - 3).map(h => (h.targetMS - h.actualMS) * (h.targetMS - h.actualMS)).sum.toDouble / history.size
      if (variance < 0.0000001) return limit

      val stdDev = Math.sqrt(variance)
      val C = limit.toDouble
      val ceffs = Stats.calc0to3(stdDev, C)
      val cffs1oAlpha = Stats.withAlpha(alpha, ceffs)
      val dy = Stats.deriviation(cffs1oAlpha(0) * 3, cffs1oAlpha(1) * 2, cffs1oAlpha(2))

      //      println(s"dy: $dy")
      if (java.lang.Double.isNaN(dy._2)) {
        1
      } else {
        Math.max(0, Math.min(limit, dy._2)).toInt
      }
      //      val values = (Seq(0.0) ++ history.map(s => (s.actualMS.toDouble - s.targetMS) / s.targetMS).filter(_ > 0)).sorted
      //      val avg = values.sum / (values.size + Double.MinPositiveValue)
      //      val discount = avg
      //      Math.ceil(target / (1 + discount)).toInt
    }

    def calcVariance(history: List[QueryStat]): Double = {
      history.takeRight(history.size - 3).map(h => (h.targetMS - h.actualMS) * (h.targetMS - h.actualMS)).sum.toDouble / history.size
      history.filter(h => h.targetMS < h.actualMS).map(h => (h.targetMS - h.actualMS) * (h.targetMS - h.actualMS)).sum.toDouble / history.size
    }

    def estimateTargetUsingHisto(limit: Int, alpha: Double, history: List[QueryStat]): Int = {
      if (history.size < 10) {
        estimateTargetUsing3function(limit, alpha, history)
      } else {
        val b = 100
        val histo = new Stats.Histogram(b)
        history.foreach(h => histo += h.actualMS - h.targetMS)

        val probs: Seq[Double] = (0 to (limit / b - 1)).map(histo.prob) ++ Seq(histo.cumProb(limit / b))
        val exp: Seq[Double] = Stats.useHistogramUniformFunction(limit, b, alpha, probs)
        //        println(s"probs: $probs")
        //        println(s"expectation: $exp")
        val maxId = exp.zipWithIndex.maxBy(_._1)._2
        val target = limit - (maxId + 1) * b / 2
        val useModel = estimateTargetUsing3function(limit, alpha, history)
        println(s"using model: $useModel, using target:$target")
        target
      }
    }

    def estimateTargetUsing3function(limit: Int, alpha: Double, history: List[QueryStat]): Int = {
      if (history.size < 3) {
        return 1
      }
      val variance = calcVariance(history)
      if (variance < 0.0000001) return limit

      val stdDev = Math.sqrt(variance)
      val C = limit.toDouble
      val c1 = Stats.constantP1(alpha, C, stdDev)
      val c2 = Stats.constantP2(alpha, C, stdDev)
      val c3 = Stats.constantP3(alpha, C, stdDev)
      val c4 = Stats.constantP4(alpha, C, stdDev)

      def fx(c: Stats.Coeff3, x: Double): Double = {
        c.a2 * x * x + c.a1 * x + c.a0
      }

      def peak(p: Double, c: Stats.Coeff3, low: Double, high: Double): (Double, Double) = {
        val vx = 1 / (p * alpha) + C - 3 * stdDev
        val vy = if (vx < high && vx > low) fx(c, vx) else Double.MinValue
        (vx, vy)
      }

      val v1T = fx(c1, C)
      val v1To = fx(c1, C - stdDev)
      val (vX1, v1X) = peak(Data.P1, c1, C - stdDev, C)

      val v2To = fx(c2, C - stdDev)
      val v2T2o = fx(c2, C - 2 * stdDev)
      val (vX2, v2X) = peak(Data.P2, c2, C - 2 * stdDev, C - stdDev)

      val v3T2o = fx(c3, C - 2 * stdDev)
      val v3T3o = c4 * (C - 3 * stdDev)
      val (vX3, v3X) = peak(Data.P3, c3, C - 3 * stdDev, C - 2 * stdDev)
      val seq = Seq(v1T, v1To, v1X, v2To, v2T2o, v2X, v3T2o, v3T3o, v3X)
      val id = seq.zipWithIndex.maxBy(_._1)._2

      val x = id match {
        case 0 =>
          C.toInt
        case 1 =>
          (C - stdDev).toInt
        case 2 =>
          vX1.toInt
        case 3 =>
          (C - stdDev).toInt
        case 4 =>
          (C - 2 * stdDev).toInt
        case 5 =>
          vX2.toInt
        case 6 =>
          (C - 2 * stdDev).toInt
        case 7 =>
          (C - 3 * stdDev).toInt
        case 8 =>
          vX3.toInt
      }

      Math.max(0, x)
    }

    def naiveFormular(targetMS: Int, history: List[QueryStat]): Int = {
      val est = targetMS * history.last.estSlice / history.last.actualMS
      //      Math.min(est, history.last.estSlice * 2)
      est
    }

    def formularUsingStatsLib(targetMS: Int, history: List[QueryStat]): Int = {
      val lastRange = history.last.estSlice
      val lastTime = history.last.actualMS
      val lastTarget = history.last.targetMS
      if (history.size < 3) {
        val nextGap = lastRange * targetMS / lastTime
        Math.min(nextGap.toInt, lastRange * 2)
      } else {

        val obs: WeightedObservedPoints = new WeightedObservedPoints()
        history.foreach(h => obs.add(h.estSlice, h.actualMS))
        val rawCoeff = Stats.linearFitting(obs)

        val coeff = if (rawCoeff.a0 < 0 || rawCoeff.a1 < 0) {
          Stats.Coeff(0, history.last.actualMS.toDouble / history.last.estSlice)
        } else {
          rawCoeff
        }

        val noise = lastTime - lastTarget
        val range = (targetMS - coeff.a0) / coeff.a1
        //        println(s"coeff:$coeff, noise: $noise, nextRange: $range")
        Math.min(range.toInt, lastRange * 2)
      }
    }

    def formular(targetMS: Int, lastGap: Int, lastTime: Long, history: List[QueryStat], lambda: Double): Int = {
      if (history.size > 3) {
        val (xa, xb) = linearInterpolation(history, history.size)
        val dist = distance(xa, xb, lastGap, lastTime)
        println(s"interpolation: b = $xb, a = $xa, lastDistance: $dist")

        val lastEST = xa + xb * lastGap
        val noise = lastTime - lastEST

        val nextGap = ((targetMS - noise - xa) / xb).toInt
        Math.min(nextGap, lastGap * 2)
      } else {
        val nextGap = lastGap * targetMS / lastTime
        Math.min(nextGap.toInt, lastGap * 2)
      }

      //      val nextGap = lambda * lastGap * requireTime / lastTime +
      //        (1 - lambda) * requireTime * histoGap * 1.0 / histoTime
      //      Math.min(nextGap.toInt, lastGap * 2)
    }
  }

  def testHistory(): Unit = {
    //  val countPerGap = 90000 // global
    //  val countPerGap = 1000 // happy
    //  val countPerGap = 250 // trump
    val countPerGap = 250
    // a
    val gap = 80
    var times = 0

    warmUp()
    1 to 30 foreach { day =>
      var start = new DateTime(2016, 9, day, 8, 0)
      val stop = start.plusHours(24)

      while (start.getMillis < stop.getMillis) {
        val aql = getAQL(start, gap, Some("happy"))
        val (time, count) = timeQuery(aql, "count")
        if (count > countPerGap * gap) {
          times += 1
          if (times > 10) {
            exit()
          }
        }
        start = start.plusHours(1)
      }
    }
  }


  def getAQL(start: DateTime, gapHour: Int, keyword: Option[String]): String = {
    val keywordFilter = keyword.map(k => FilterStatement(TextField("text"), None, Relation.contains, Seq(k)))
    val timeFilter = FilterStatement(TimeField("create_at"), None, Relation.inRange,
      Seq(TimeField.TimeFormat.print(start),
        TimeField.TimeFormat.print(start.plusHours(gapHour))))
    val byHour = ByStatement(TimeField("create_at"), Some(Interval(TimeUnit.Minute, 10 * gapHour)), Some(NumberField("hour")))
    val groupStatement = GroupStatement(Seq(byHour), Seq(aggrCount))
    //      val query = Query(dataset = "twitter.ds_tweet", filter = Seq(timeFilter), groups = Some(groupStatement))
    val query = Query(dataset = "twitter.ds_tweet", filter = keywordFilter.map(Seq(timeFilter, _)).getOrElse(Seq(timeFilter)), globalAggr = Some(globalAggr))
    gen.generate(query, TwitterDataStore.TwitterSchema)
  }

  def getSeqTimeAQL(timeSeq: Seq[(DateTime, DateTime)], keyword: String): Seq[String] = {
    val keywordFilter = FilterStatement(TextField("text"), None, Relation.contains, Seq(keyword))
    val timeFilters = timeSeq.map { case (start, end) =>
      FilterStatement(TimeField("create_at"), None, Relation.inRange, Seq(TimeField.TimeFormat.print(start), TimeField.TimeFormat.print(end)))
    }
    timeFilters.map { f =>
      val query = Query(dataset = "twitter.ds_tweet", filter = Seq(keywordFilter, f), globalAggr = Some(globalAggr))
      gen.generate(query, TwitterDataStore.TwitterSchema)
    }
  }

  def getCountKeyword(keyword: String): String = {
    val keywordFilter = FilterStatement(TextField("text"), None, Relation.contains, Seq(keyword))
    val query = Query(dataset = "twitter.ds_tweet", filter = Seq(keywordFilter), globalAggr = Some(globalAggr))
    gen.generate(query, TwitterDataStore.TwitterSchema)
  }

  def getCountTime(start: DateTime, end: DateTime): String = {
    val timeFilter = FilterStatement(TimeField("create_at"), None, Relation.inRange,
      Seq(TimeField.TimeFormat.print(start),
        TimeField.TimeFormat.print(end)))
    val query = Query(dataset = "twitter.ds_tweet", filter = Seq(timeFilter), globalAggr = Some(globalAggr))
    gen.generate(query, TwitterDataStore.TwitterSchema)
  }

  def getTopK: String = {
    val query = Query(
      dataset = "twitter.ds_tweet",
      unnest = Seq(UnnestStatement(BagField("hashtags", DataType.String), StringField("tag"))),
      groups = Some(
        GroupStatement(
          Seq(ByStatement(StringField("tag"), None, None),
            ByStatement(TimeField("create_at"), Some(Interval(TimeUnit.Day)), Some(NumberField("day")))),
          Seq(AggregateStatement(AllField, Count, NumberField("count")))
        )))
    gen.generate(query, TwitterDataStore.TwitterSchema)
  }

  def getSamplePerDay(generator: AQLGenerator, start: DateTime, end: DateTime): String = {
    import TimeField.TimeFormat
    val query = Query(
      dataset = "twitter.ds_tweet",
      filter = Seq(
        FilterStatement(TimeField("create_at"), None, Relation.inRange, Seq(TimeFormat.print(start), TimeFormat.print(end)))
      ),
      select = Some(SelectStatement(orderOn = Seq.empty, order = Seq.empty, limit = 1, offset = 0, fields = Seq(TimeField("create_at"), NumberField("id"))))
    )
    generator.generate(query, TwitterDataStore.TwitterSchema)
  }

  def searchBreakdown(generator: AQLGenerator): Unit = {

    def countAQL(key: String, s: DateTime, e: DateTime) =
      s"""
         |{
         |'count':
         |count(
         |for $$t in dataset twitter.ds_tweet
         |where $$t.create_at >= datetime('${TimeField.TimeFormat.print(s)}')
         |and   $$t.create_at < datetime('${TimeField.TimeFormat.print(e)}')
         |and similarity-jaccard(word-tokens($$t.'text'), word-tokens('$key')) > 0.0
         |return $$t.id
         |)
         |}
       """.stripMargin

    def countAQLByTime(s: DateTime, e: DateTime) =
      s"""
         |{
         |'count':
         |count(
         |for $$t in dataset twitter.ds_tweet
         |where $$t.create_at >= datetime('${TimeField.TimeFormat.print(s)}')
         |and   $$t.create_at < datetime('${TimeField.TimeFormat.print(e)}')
         |return $$t.id
         |)
         |}
       """.stripMargin

    def sortAQL(key: String, s: DateTime, e: DateTime) =
      s"""
         |{
         |'count':
         |count(
         |for $$t in dataset twitter.ds_tweet
         |where $$t.create_at >= datetime('${TimeField.TimeFormat.print(s)}')
         |and   $$t.create_at < datetime('${TimeField.TimeFormat.print(e)}')
         |and similarity-jaccard(word-tokens($$t.'text'), word-tokens('$key')) > 0.0
         |order by $$t.create_at
         |return $$t.id
         |)
         |}
       """.stripMargin

    def getIDsAQL(key: String, s: DateTime, e: DateTime) =
      s"""
         |for $$t in dataset twitter.ds_tweet
         |where $$t.create_at >= datetime('${TimeField.TimeFormat.print(s)}')
         |and   $$t.create_at < datetime('${TimeField.TimeFormat.print(e)}')
         |and similarity-jaccard(word-tokens($$t.'text'), word-tokens('$key')) > 0.0
         |order by $$t.id
         |return $$t.id
       """.stripMargin

    def idToPIndexAQL(seqId: Seq[Long], s: DateTime, e: DateTime) =
      s"""
         |{
         |'count':
         |count(
         |for $$id in [ ${seqId.mkString(",")} ]
         |for $$t in dataset twitter.ds_tweet
         |where $$id /* +indexnl */ = $$t.id
         |and $$t.create_at >= datetime("${TimeField.TimeFormat.print(s)}")
         |and $$t.create_at <  datetime("${TimeField.TimeFormat.print(e)}")
         |return $$t.id
         |)
         |}
       """.stripMargin

    def getIDs(aql: String): Seq[Long] = {
      val f = asterixConn.postQuery(aql).map { ret =>
        ret.as[Seq[Long]]
      }
      Await.result(f, scala.concurrent.duration.Duration.Inf)
    }

    def runOnePair(key: String, s: DateTime, e: DateTime): (Long, Double, Long, Double, Long, Double, Int) = {
      if (s.getMillis >= e.getMillis) {
        return (0, 0.0, 0, 0.0, 0, 0.0, 0)
      }
      println(s"start:${TimeField.TimeFormat.print(s)},end:${TimeField.TimeFormat.print(e)}")

      //      println("start full count query")
      //      val qCount = countAQL(key, s, e)
      val qCount = countAQLByTime(s, e)
      val (f1, avg1, count) = multipleTime(2, qCount)
      println(s"firstTime\t$f1\tavg\t$avg1\tcount\t$count")

      //     println("+sort")
      //     val qSort = sortAQL(key, s, e)
      //     val (f2, avg2, count2) = multipleTime(3, qSort)
      //     println(s"firstTime\t$f2\tavg\t$avg2\tcount\t$count2")
      //
      //     println("start full count query")
      //     val qCount = countAQL(key, s, e)
      //     val (f1, avg1, count) = multipleTime(3, qCount)
      //     println(s"firstTime\t$f1\tavg\t$avg1\tcount\t$count")
      //
      //     val ids = getIDs(getIDsAQL(key, s, e))
      //     println("primary index search only")
      //     val qPIdx = idToPIndexAQL(ids, s, e)
      //     val (f3, avg3, count3) = multipleTime(3, qPIdx)
      //     println(s"firstTime\t$f3\tavg\t$avg3\tcount\t$count3")
      //
      //     (f1, avg1, f2, avg2, f3, avg3, count)
      (f1, avg1, 0, 0.0, 0, 0.0, count)
    }

    val hours = new Duration(urStartDate, urEndDate).toStandardHours.getHours

    //        for (key <- keywords) {
    for (key <- Seq("trump")) {
      //      0 to 7 foreach { base =>
      //      7 to 7 foreach { base =>
      //        val slice = 1 << base
      for (slice <- 200 to 300 by 10) {
        //      for (slice <- 250 to 250) {
        //        println(s"slice:$slice")
        val step = hours / slice
        var (sumCountFirst, sumSortFirst, sumPIdxFirst) = (0.0, 0.0, 0.0)
        var (sumCountAvg, sumSortAvg, sumPIdxAvg) = (0.0, 0.0, 0.0)
        Stream.iterate(urStartDate)(s => s.plusHours(step)).takeWhile(_.getMillis < urEndDate.getMillis).foreach { s =>
          val (firstCount, avgCount, firstSort, avgSort, firstPIdx, avgPIdx, _) = runOnePair(key, s, Seq(s.plusHours(step), urEndDate).min)
          sumCountFirst += firstCount
          sumSortFirst += firstSort
          sumPIdxFirst += firstPIdx
          sumCountAvg += avgCount
          sumSortAvg += avgSort
          sumPIdxAvg += avgPIdx
        }
        //        println(
        //          s"""
        //             |$key, slice:$slice, average: count only time:$sumCountAvg, with sort time:$sumSortAvg, sort self:${sumSortAvg - sumCountAvg}, pIndex:$sumPIdxAvg")
        //             |$key, slice:$slice, first  : count only time:$sumCountFirst, with sort time:$sumSortFirst, sort self:${sumSortFirst - sumCountFirst}, pIndex:$sumPIdxFirst")
        //           """.stripMargin)
        println(
          s"""
             |$key, slice:$slice, cold time:$sumCountFirst, cold avg: ${sumCountFirst * 1.0 / slice}
             |$key, slice:$slice, hot time:$sumCountAvg, hot avg: ${sumCountAvg * 1.0 / slice}
             |""".stripMargin)
      }
    }
  }

  def pIndexOnly: Unit = {
    def toAQL(seqId: Seq[Long], s: DateTime, e: DateTime) =
      s"""
         |{
         |'count':
         |count(
         |for $$id in [ ${
        seqId.mkString(",")
      } ]
         |for $$t in dataset twitter.ds_tweet
         |where $$id = $$t.id
         |and $$t.create_at >= datetime("${
        TimeField.TimeFormat.print(s)
      }")
         |and $$t.create_at <  datetime("${
        TimeField.TimeFormat.print(e)
      }")
         |return $$t.id
         |)
         |}
       """.stripMargin

    val start = new DateTime(2016, 1, 1, 0, 0)

    Stream.iterate(365)(n => n / 2).takeWhile(_ > 0).foreach {
      slices =>

        val days = 365 / slices

        println(s"slice:$slices")
        var (sumFirst, sumAvg) = (0.0, 0.0)
        0 to 364 by days foreach {
          offset =>
            val ids = Data.ids.slice(offset, offset + days)
            val s = start.plusDays(offset)
            val e = Seq(s.plusDays(days), start.plusYears(1)).min
            val aql = toAQL(ids, s, e)
            val (first, avg, count) = multipleTime(1, aql)
            println(s"days: $days, first: $first, avg: $avg, count: $count")
            sumFirst += first
            sumAvg += avg
        }
        println(s"slice:$slices, first: $sumFirst, avg: $sumAvg")
    }
  }

  exit()
}

object GetSampleID extends App {
  val gen = new AQLGenerator()
  val start = new DateTime(2016, 1, 1, 0, 0)
  0 to 365 foreach { i =>
    println(ResponseTime.getSamplePerDay(gen, start.plusDays(i), start.plusDays(i + 1)).split("\\\n").filterNot(_.isEmpty).mkString("\n"))
  }
}

object Stats extends App {

  import scala.collection.JavaConverters._

  /**
    * a0 + a1 * x
    */
  case class Coeff(a0: Double, a1: Double)

  case class Coeff3(a2: Double, a1: Double, a0: Double) {
    override def toString: String =
      s"""
         |a2=$a2
         |a1=$a1
         |a0=$a0
       """.stripMargin
  }

  def linearFitting(obs: WeightedObservedPoints): Coeff = {
    val filter: PolynomialCurveFitter = PolynomialCurveFitter.create(1)
    val ret = filter.fit(obs.toList)
    Coeff(ret(0), ret(1))
  }

  def calcVariance(obs: WeightedObservedPoints, coeff: Coeff): Double = {
    val list = obs.toList.asScala
    var variance = 0.0
    for (ob: WeightedObservedPoint <- list) {
      val y = coeff.a0 + coeff.a1 * ob.getX
      variance += (y - ob.getY) * (y - ob.getY)
    }
    variance / list.size
  }

  def calcLinearCoeff(x1: Double, y1: Double, x2: Double, y2: Double): Coeff = {
    val a1 = (y2 - y1) / (x2 - x1)
    val a0 = y1 - a1 * x1
    Coeff(a0, a1)
  }

  def calcThreeAppxLine(variance: Double): Seq[Coeff] = {
    val stdDev = Math.sqrt(variance)
    val norm = new NormalDistribution(null, 0, stdDev)
    println(norm.density(0), norm.density(stdDev), norm.density(stdDev * 2), norm.density(stdDev * 3))
    val coeff1 = calcLinearCoeff(0, norm.density(0), stdDev, norm.density(stdDev))
    val coeff2 = calcLinearCoeff(stdDev, norm.density(stdDev), 2 * stdDev, norm.density(2 * stdDev))
    val coeff3 = calcLinearCoeff(stdDev * 2, norm.density(stdDev * 2), 3 * stdDev, norm.density(3 * stdDev))
    Seq(coeff1, coeff2, coeff3)
  }

  def calc0to1(gaussianCoeff: Seq[Coeff], stdDev: Double, C: Double): Seq[Double] = {
    val o = stdDev
    val o2 = stdDev * stdDev
    val o3 = stdDev * o2
    val C2 = C * C
    val C3 = C * C2

    val a1 = gaussianCoeff(0).a1
    val b1 = gaussianCoeff(0).a0
    val a2 = gaussianCoeff(1).a1
    val b2 = gaussianCoeff(1).a0
    val a3 = gaussianCoeff(2).a1
    val b3 = gaussianCoeff(2).a0

    val c3 = a1 / 3
    val c2 = a1 * o + (b1 - C * a1) / 2 + a2 * stdDev + a3 * stdDev

    // without alpha
    val c1 = o2 * (a1 + 9) + o * (b1 - C * (a1 + a2 + a3) + 2 * a2 * 4 * a3) - b1 * C

    val c0 = o3 * (a1 / 3 + a2 + 4 * a3 + 11) + o2 * (1 / 2 * (b1 - C * a1) + 3 / 2 * (b2 - C * a2) + 5 / 2 * (b3 - C * a3)) - o * C * (b1 + b2 + b3) - a1 / 3 * C3 + (b1 - C * a1) / 2 * C2 - b1 * C2

    Seq(c3, c2, c1, c0)
  }

  def calc0to3(stdDev: Double, C: Double): Seq[Double] = {
    val norm = new NormalDistribution(null, 0, stdDev)
    val coeffGauss = calcLinearCoeff(0, norm.density(0), stdDev * 3, norm.density(stdDev * 3))
    //    val coeffGauss = calcLinearCoeff(0, 0.5, stdDev * 3, 0)
    val a = coeffGauss.a1
    val o = stdDev
    val o2 = o * o
    val o3 = o * o2

    val C2 = C * C
    val C3 = C * C2

    val c3 = -a / 6
    val c2 = 0.5 * a * C - 1.5 * a * o
    val c1 = -10.5 * a * o2 + 3 * a * C * o - 0.5 * a * C2
    val c0 = 4.5 * a * C * o2 - 4.5 * a * o3 + 1 / 6 * a * C3 - 1.5 * a * o * C2
    Seq(c3, c2, c1, c0)
  }

  def maxAlpha(stdDev: Double, C: Double): Double = {
    val norm = new NormalDistribution(null, 0, stdDev)
    val coeffGauss = calcLinearCoeff(0, norm.density(0), stdDev * 3, norm.density(stdDev * 3))
    val b1 = coeffGauss.a1
    val o = stdDev
    val o2 = o * o
    val C2 = C * C
    val left = (9 * (o - C) * (o - C) - 21 * o2 + 6 * C * o - C2)
    2 / (b1 * left)
  }

  def withAlpha(alpha: Double, ceffs: Seq[Double]): Seq[Double] = {
    val c3 = -alpha * ceffs(0)
    val c2 = -alpha * ceffs(1)
    val c1 = 1 - alpha * ceffs(2)
    val c0 = -alpha * ceffs(3)
    Seq(c3, c2, c1, c0)
  }

  def deriviation(a: Double, b: Double, c: Double): (Double, Double) = {
    ((-b + Math.sqrt(b * b - 4 * a * c)) / (2 * a),
      (-b - Math.sqrt(b * b - 4 * a * c)) / (2 * a))
  }


  def mulAlpha(alpha: Double, p: Double, a2: Double, a1: Double, a0: Double): Coeff3 = {
    Coeff3(-p * alpha * a2, 1 - p * alpha * a1, -p * alpha * a0)
  }

  def constantP1(alpha: Double, c: Double, o: Double): Coeff3 = {
    val a2: Double = 0.5
    val a1: Double = 3 * o - c
    val a0: Double = Math.pow(2 * o - c, 2) + 0.5 * o * o
    mulAlpha(alpha, Data.P1, a2, a1, a0)
  }

  def constantP2(alpha: Double, c: Double, o: Double): Coeff3 = {
    val a2 = 0.5
    val a1 = 3 * o - c
    val a0 = 0.25 * o * o + Math.pow(2.5 * o - c, 2)
    mulAlpha(alpha, Data.P2, a2, a1, a0)
  }

  def constantP3(alpha: Double, c: Double, o: Double): Coeff3 = {
    val a2 = 0.5
    val a1 = 3 * o - c
    val a0 = a1 * a1
    mulAlpha(alpha, Data.P3, a2, a1, a0)
  }

  def constantP4(alpha: Double, c: Double, o: Double): Double = {
    1 - alpha * Data.P3 / 2 * (c - 3 * o)
  }

  def constantDev(alpha: Double, p: Double, o: Double, c: Double): Double = {
    1 / (alpha * p) - 3 * o + c
  }


  //  val gaussianCoeff = calcThreeAppxLine(variance)

  //  val cffs1o = calc0to1(gaussianCoeff, stdDev, 2)

  def useOneLinearFunction(C: Double, stdDev: Double, alpha: Double): Unit = {
    val ceffs = calc0to3(stdDev, C)
    val cffs1oAlpha = withAlpha(alpha, ceffs)
    val dy = deriviation(cffs1oAlpha(0) * 3, cffs1oAlpha(1) * 2, cffs1oAlpha(2))

    val strSigma = f"$stdDev%1.2f"
    val strAlpha = f"$alpha%1.1f"

    println(variance, stdDev)
    println(s"c - o:${C - stdDev}")
    println(ceffs)
    println(cffs1oAlpha)
    println(s"maxAlpha ${maxAlpha(stdDev, C)}")
    println(
      s"""
         |set key left box
         |set autoscale
         |set samples 800
         |set terminal postscript eps enhanced size 3in,3in
         |
         |y3(a3, a2, a1, a0, x) = a3* (x**3) + a2 * (x**2) + a1 * x + a0
         |plot [${C - 3 * stdDev}:$C] y3(${cffs1oAlpha(0)}, ${cffs1oAlpha(1)}, ${cffs1oAlpha(2)}, ${cffs1oAlpha(3)}, x) \\
         |      ti '{/Symbol a}=$strAlpha, {/Symbol o}=$strSigma'
         |""".stripMargin
    )
    println(dy)

  }

  def useNormalizedLinearFunction(C: Double, stdDev: Double, alpha: Double): Unit = {
    val norm = new NormalDistribution(0, stdDev)
    val b = norm.density(0)
    val b2 = b * b
    val c2 = C * C
    val c3 = C * c2
    val c0 = -alpha * (-b2 / 6 * c3 + b / 2 * c2 - C / 2 + 1 / (6 * b))
    val x0 = 1 / b
    println(s"x0 = $x0, c0 = $c0, stdDev:$stdDev")
    println(
      s"""
         |
         |set key left box
         |set autoscale
         |set samples 800
         |set terminal postscript eps enhanced size 3in,3in
         |plot [${C - x0}:$C] x+$c0
       """.stripMargin)
  }

  class Histogram(step: Double) {
    require(step > 0)
    private var sum: Double = 0.0
    private val posCounts = new ArrayBuffer[Int]
    private val negCounts = new ArrayBuffer[Int]

    def +=(value: Double): Unit = {
      val id = (value / step).toInt
      val count = if (value >= 0) posCounts else negCounts
      val absId = if (value >= 0) id else -id
      count.size to absId foreach (_ => count += 0)
      count(absId) += 1
      sum += 1
    }

    def prob(id: Int): Double = {
      val (rid, count) = if (id >= 0) (id, posCounts) else (-id, negCounts)
      if (rid < count.size) {
        count(rid) / sum
      } else {
        0.0
      }
    }

    /**
      * cumunitative probs >= id
      *
      * @param id
      * @return
      */
    def cumProb(id: Int): Double = {
      val (rid, count) = if (id >= 0) (id, posCounts) else (-id, negCounts)
      if (rid < count.size) {
        val s = count.slice(rid, count.size).sum
        s / sum
      } else {
        0
      }
    }
  }

  def useHistogramUniformFunction(W: Double, b: Double, alpha: Double, probs: Seq[Double]): Seq[Double] = {
    val b2 = b * b
    val seqIntegral: Seq[Double] = 1 to probs.size map (i => 0.5 * b2 + (i - 1) * b2)
    //    println(seqIntegral)

    def value(j: Int) = probs.slice(j, probs.size).zip(seqIntegral).map { case (p: Double, v: Double) => p * v }.sum

    //    0 to (probs.size - 1) foreach (j => println(s"gain:${W - j * b}, penalty:alpha * ${value(j)} = ${alpha * value(j)}"))

    0 to (probs.size - 1) map (j => W - j * b - alpha * value(j))
  }

  def use3UniformFunction(C: Double, stdDev: Double, alpha: Double): Unit = {
    val end = 3
    val c1 = constantP1(alpha, C, stdDev)
    val c2 = constantP2(alpha, C, stdDev)
    val c3 = constantP3(alpha, C, stdDev)
    val c4 = constantP4(alpha, C, stdDev)

    println(C - stdDev, C)
    println(c1)
    println(C - 2 * stdDev, C - stdDev)
    println(c2)
    println(C - 3 * stdDev, C - 2 * stdDev)
    println(c3)
    println("dy1=" + constantDev(alpha, Data.P1, stdDev, C))
    println(" dy2=" + constantDev(alpha, Data.P2, stdDev, C))
    println(" dy3=" + constantDev(alpha, Data.P3, stdDev, C))

    val strSigma = f"$stdDev%1.2f"
    val strAlpha = f"$alpha%1.1f"

    println(
      s"""
         |set key left box
         |set autoscale
         |set samples 800
         |set terminal postscript eps enhanced size 3in,3in
         |
         |y2(a2,a1,a0, x) = a2 * (x**2) + a1 * x + a0
         |plot [${C - 3 * stdDev}:$C] x > $C ? 0 : ( ${C - stdDev} < x && x <= $C ? y2(${c1.a2}, ${c1.a1}, ${c1.a0}, x) \\
         |             :(${C - 2 * stdDev} < x && x <= ${C - stdDev} ? y2(${c2.a2}, ${c2.a1}, ${c2.a0}, x) \\
         |                :(${C - 3 * stdDev} < x && x <= ${C - 2 * stdDev} ? y2(${c3.a2}, ${c3.a1}, ${c3.a0}, x) : $c4*x))) \\
         |                ti '{/Symbol a}=$strAlpha, {/Symbol o}=$strSigma'
       """.stripMargin)
  }

  val obs: WeightedObservedPoints = new WeightedObservedPoints()
  Seq((1, 0.5), (7, 3.9), (2, 1.8)).foreach(x => obs.add(x._1, x._2))
  val coeff = linearFitting(obs)
  println(coeff)
  val variance = calcVariance(obs, coeff)
  //    val variance = 0.25
  val stdDev = Math.sqrt(variance)
  println(variance, stdDev)

  val C = 2.2
  val alpha = 1
  //  useNormalizedLinearFunction(C, stdDev, alpha)
  //    useOneLinearFunction(C, stdDev, alpha)
  //  use3UniformFunction(C, stdDev, alpha)
  //  val px = useHistogramUniformFunction(2, 0.5, Seq(0.26, 0.35, 0.24, 0.01))
  //  println(px)
}

object Data {
  val ids = Seq(682833636383522817l, 683196024362524672l, 683558412152487936l, 683920800517238784l, 684283187854503936l, 684645575917191168l, 685007964231450626l, 685370351824441344l, 685732739362983936l, 686095127274745856l, 686457515417202689l, 686819903249096704l, 687182290766544896l, 687544679957565440l, 687907067114450944l, 688269454833135616l, 688631842329595904l, 688994230262366208l, 689356618866245632l, 689719005855100928l, 690081393783758848l, 690443781666140161l, 690806169850351616l, 691168557384798208l, 691530945178898432l, 691893335703814144l, 692255721619066880l, 692618109736218624l, 692980497312514048l, 693342885450760192l, 693705272582348800l, 694067660364062720l, 694430049261506565l, 694792436619595776l, 695154823843319808l, 695517211721654272l, 695879600660819968l, 696241987389751296l, 696604375859359744l, 696966763213246464l, 697329151716433920l, 697691538931867648l, 698053927367999488l, 698416315061514244l, 698778702654603264l, 699141090457194497l, 699503478377312256l, 699865866180042752l, 699965866180042752l, 699985866180042752l, 699995866180042752l, 700088225913208833l, 701888225913208833l, 702040193264541696l, 702402581679742976l, 702764969058820096l, 703127357566033920l, 703489744789962752l, 703852132974211076l, 704214520797868033l, 704576909309444096l, 704939296193699840l, 705301684151558144l, 705664072113455104l, 706026460461436928l, 706388848801161216l, 706751235710300160l, 707113623680737281l, 707476011516960768l, 707838399105912832l, 708200787415937024l, 708563174950211584l, 708925563100987393l, 709272851065524224l, 709635239069351936l, 709997626947719168l, 710360015190646785l, 710460015190646785l, 710560015190646785l, 710660015190646785l, 710760015190646785l, 710860015190646785l, 710960015190646785l, 711060015190646785l, 711160015190646785l, 711260015190646785l, 711360015190646785l, 711460015190646785l, 711560015190646785l, 711660015190646785l, 715635459397967872l, 715795833350959104l, 716158221178634241l, 716520608746450944l, 716882996301705218l, 717245385140342784l, 717607772418883584l, 717970159994929153l, 718332548129157120l, 718694935755489280l, 719057323486875648l, 719419711373463552l, 719782099222310912l, 720144487243120640l, 720506875096276992l, 720869263913783296l, 721231651112488961l, 721594038634164224l, 721956427716124673l, 722318814898065408l, 722681202109521920l, 723043589991751680l, 723405978645893121l, 723768365982994432l, 724130753819324416l, 724493141533925378l, 724855529445662720l, 725217917277626368l, 725580305139118080l, 725942693415636992l, 726305080828293120l, 726667468861771776l, 727029856836444161l, 727392244345397252l, 727754632425054209l, 728117020256964608l, 728479407954763777l, 728841796021682176l, 729204183736291328l, 729566572063248385l, 729928959509471232l, 730291347458916352l, 730653735232266241l, 731016123488014336l, 731378511475281920l, 731740898745155584l, 732103286627520512l, 732465674694451200l, 732828062736338944l, 733190450325032962l, 733552838144450560l, 733915225921970180l, 734277613884190720l, 734640001921757184l, 735002389715910656l, 735364777799733248l, 735727165497442304l, 736089553325281281l, 736451941061042176l, 736814328964218880l, 737176716871761920l, 737539104741556224l, 737901492447907840l, 738263880514736128l, 738626268300607488l, 738988656141049858l, 739351043973251072l, 739713432023269377l, 740075819754655744l, 740438207511269378l, 740800595376939009l, 741162983603179520l, 741525371107966976l, 741887759330082816l, 742250146860146688l, 742612534788653056l, 742974922692173824l, 743337310708604930l, 743699698343510016l, 744062086490300417l, 744424474104184832l, 744786861898399744l, 745149249814290432l, 745511637637947392l, 745874025872769024l, 746236413427912705l, 746598801427693569l, 746961189175861248l, 747323577003761673l, 747685964923932672l, 748048352693133313l, 748595703279136768l, 748773128977866753l, 749135516306509828l, 749497904352653312l, 749860292188790784l, 750222680440180736l, 750585068138205185l, 750947455764758528l, 751309843613356032l, 751672231437164544l, 752034619344564224l, 752397007134658563l, 752759394950012928l, 753121782895370240l, 753484170744041473l, 753846558941073408l, 754208946458681344l, 754571334613512192l, 754933722260967425l, 755296110143471616l, 755658497920995328l, 756020885916491777l, 756383274432204801l, 756745661605941248l, 757108049576206336l, 757470437446082560l, 757832825168990208l, 758195212988588032l, 758557600942264320l, 758919988891660288l, 759282376694521856l, 759644764383813633l, 760007152450740224l, 760369540186288128l, 760731928911904772l, 761094315921645568l, 761456704223444992l, 761819091736723456l, 762181479652745216l, 762543867430264832l, 762906255341998080l, 763268643295596545l, 763631031115128832l, 763993419064549376l, 764355806909370368l, 764928744683245568l, 765080582510358529l, 765442970560630784l, 765805358292033536l, 766167746220482560l, 766530133834338304l, 766892522090160128l, 767441549806346240l, 767617298324721664l, 767979685741531136l, 768342073271394304l, 768704461032321024l, 769066850294562816l, 769429236901961728l, 769929037821075456l, 770154012637265920l, 770516400485961728l, 770878788838326272l, 771241176775036932l, 771603563986300928l, 771965952099491840l, 772440663098990592l, 772690727960653824l, 773053115603877888l, 773415503352229888l, 773938687327477761l, 774140279364341760l, 774502667099910145l, 774865055083036673l, 775227442722054144l, 775609198147096576l, 775952218348400640l, 776314606981619712l, 776676994721406976l, 777039382121439232l, 777401770033020929l, 777764158213271552l, 778126546175455232l, 778488933961400320l, 778851321755471872l, 779213709935775744l, 779576097461506048l, 779938485281103872l, 780300872785862657l, 780663260768940032l, 781025648634716160l, 781388036516880385l, 781750424504143872l, 782112812558610432l, 782475200034181121l, 782837587790659584l, 783199976201465856l, 783562363890839552l, 783924752922451968l, 784287139735359492l, 784649528179777537l, 785011916255088641l, 785374302971408384l, 785736691072073728l, 786099079105376256l, 786461467310694401l, 786823854903525380l, 787186242211282944l, 787548630064390144l, 787911018836099072l, 788273405954920448l, 788635793917112320l, 788998182080393216l, 789360569728065536l, 789460569728065536l, 789560569728065536l, 789660569728065536l, 791046721530920961l, 791172508762255361l, 791534897496072192l, 791897284615122944l, 792259672388632580l, 792622060157542401l, 792984448656547840l, 793346835893157888l, 793709223754420225l, 794071612001808384l, 794433999791915008l, 794796387351207936l, 795158776722505728l, 795536262669053952l, 795898650560000000l, 796261038324711424l, 796623426668498945l, 796985814534197248l, 797348202081095680l, 797710589971853312l, 798072978315456512l, 798435365698883584l, 798797753518170114l, 799160141752991748l, 799522529132085248l, 799884917291229184l, 800247305093967873l, 800609693215297536l, 800972080971857920l, 801334468577525762l, 801696856296148992l, 802059244229033984l, 802421632254046208l, 802784020132196352l, 803146408173785089l, 803508795632553984l, 804207381714497536l, 804233572097949696l, 804595959250481152l, 804958347334131712l, 805320735170248704l, 805683122855669760l, 806045510880686082l, 806407899174105088l, 807132674573893632l, 807495062535991296l, 807857450741329920l, 808219838053126144l, 808582226006970368l, 808944614124163072l, 809307001700237312l, 809669390425735168l, 810031777410486272l, 810394165053771776l, 810756553011699712l, 811118941032644608l, 811481328743055360l, 811843717216800768l, 812206104482430976l, 812568492440489985l, 812930880289247232l, 813293268276510720l, 813655656859193344l, 814018043886104576l, 814380431864799232l, 814742819508199424l, 815105207428259845l)
  val P1: Double = 0.341
  val P2: Double = 0.136
  val P3: Double = 0.021
}

object TestDouble extends App {
  val p = Data.P1

  val obs: WeightedObservedPoints = new WeightedObservedPoints()

  // Example distribution
  Seq[(Double, Double)](
    (7.8, 5.071),
    (3.2, 2.302),
    (2.6, 2.305),
    (1.6, 2.151),
    (0.8, 1.331),
    (1, 1.431),
    (1.6, 1.842),
    (2.2, 2.764),
    (1.4, 1.149),
    (3.4, 2.809),
    (2.4, 2.183),
    (2, 2.457),
    (1, 1.536),
    (1.2, 1.227),
    (2.6, 3.071),
    (1.2, 1.535),
    (1.6, 1.193),
    (3.8, 3.154),
    (2, 1.796),
    (2.2, 2.186),
    (1.8, 1.498),
    (2.6, 2.764),
    (1.4, 1.284),
    (2.4, 2.709),
    (1.2, 1.394),
    (1.8, 1.676),
    (2.6, 3.072),
    (1, 1.546),
    (1, 1.217),
    (2, 2.575),
    (1.4, 2.564),
    (0.8, 1.824),
    (0.6, 1.939)
  ) //.foreach( x=>obs.add(x._1.toDouble, x._2.toDouble))

  Seq[(Double, Double)](
    (23.2, 3.615),
    (39, 6.773),
    (6.4, 0.919),
    (16, 2.072),
    (17.6, 2.241),
    (16.4, 1.521),
    (28.2, 4.913),
    (0.2, 0.082),
    (4.2, 1.182),
    (9.6, 2.538),
    (8.4, 2.018),
    (9.4, 2.681),
    (5.8, 2.031),
    (4.6, 0.953),
    (13.4, 2.328),
    (14.2, 2.457),
    (12, 1.767),
    (16, 2.909),
    (8, 1.835),
    (7, 1.465),
    (10.4, 2.828),
    (5, 1.162),
    (9.6, 2.563),
    (6.2, 1.739),
    (7, 2.101),
    (6.2, 2.3),
    (4.2, 1.496),
    (6, 3.007),
    (2, 0.917),
    (4.8, 2.25),
    (4.2, 1.947),
    (4.4, 2.662),
    (2.2, 2.76)
  ).foreach(x => obs.add(x._1.toDouble, x._2.toDouble))

  val coeff = Stats.linearFitting(obs)
  println(coeff)
}

object TestHistorgram extends App {
  val b = 0.5
  val alpha = 0.1
  val W = 2
  val histo = new Stats.Histogram(b)
  Seq(-1792, -907, -1040, 383, 290, 414, 1098, 637, 3000).foreach(d => histo += (d / 1000.0))
  val n = (W / b).toInt
  val probs = ((0 to (n - 1)).map(histo.prob) ++ Seq(histo.cumProb(n)))
  println(probs)
  val exp = Stats.useHistogramUniformFunction(W, b, alpha, probs)
  println(exp)
}
