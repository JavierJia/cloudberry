package edu.uci.ics.cloudberry.zion.experiment

import java.io.File
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import edu.uci.ics.cloudberry.zion.model.impl.{AsterixSQLPPConn, SQLPPGenerator}
import edu.uci.ics.cloudberry.zion.model.schema._
import org.apache.commons.math3.fitting.WeightedObservedPoints
import org.asynchttpclient.AsyncHttpClientConfig
import org.joda.time.{DateTime, Duration}
import play.api.libs.ws.WSConfigParser
import play.api.libs.ws.ahc.{AhcConfigBuilder, AhcWSClient, AhcWSClientConfig}
import play.api.{Configuration, Environment, Mode}

import scala.concurrent.{Await, ExecutionContext, Future}

trait Connection {

  val contextId = 1
  implicit val cmp: Ordering[DateTime] = Ordering.by(_.getMillis)
  val urStartDate = new DateTime(2016, 11, 4, 15, 0)
  val urEndDate = new DateTime(2017, 1, 17, 6, 0)
  //{ "$1": "2017-01-21T07:47:36.000Z", "$2": 119945355 }
  //  val numberLSMs = 965
  val numberLSMs = 267
  val timeRange: Double = 1572.0
  //  val unit = new Duration(urStartDate, urEndDate).dividedBy(numberLSMs).getStandardHours
  val unit = 1

  //  val keywords = Seq("zika", "flood", "election", "clinton", "trump", "happy", "")
  val keywords = Seq("zika", "flood", "rain", "election", "clinton", "trump", "")

  val queryGen = new SQLPPGenerator()
  val aggrCount = AggregateStatement(AllField, Count, NumberField("count"))
  val globalAggr = GlobalAggregateStatement(aggrCount)

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))
  val customConf = ConfigFactory.parseString(
    """
      |akka.log-dead-letters-during-shutdown = off
      |akka.log-dead-letters = off
      |""".stripMargin)
  implicit val system = ActorSystem("slicing", ConfigFactory.load(customConf))
  implicit val mat = ActorMaterializer()
  val wsClient = produceWSClient()
  //  val url = "http://uranium.ics.uci.edu:19002/aql"
  val url = "http://uranium.ics.uci.edu:19002/query/service"
  val cancelURL = "http://uranium.ics.uci.edu:19002/admin/requests/running"
  val adbConn = new AsterixSQLPPConn(url, wsClient)

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
        |
        |
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
    system.terminate()
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
    val f = adbConn.postQuery(aql, Some(contextId)).map { ret =>
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
        adbConn.postQuery(aql).map { ret => (ret \\ field).map(_.as[Int]).sum }
      }
      val counts = Await.result(f, scala.concurrent.duration.Duration.Inf)
      (DateTime.now.getMillis - start.getMillis, counts)
    } else {
      val counts = Seq.newBuilder[Int]
      aqls.foreach { aql =>
        val f = adbConn.postQuery(aql).map(ret => (ret \\ field).map(_.as[Int]).sum)
        val c = Await.result(f, scala.concurrent.duration.Duration.Inf)
        counts += c
      }
      (DateTime.now.getMillis - start.getMillis, counts.result())
    }
  }

}

object ResponseTime extends App with Connection {

  import Common._
  //  TestExternalSort(gen)
  //  testID
  //  exit()

  //    warmUp()
  //  searchBreakdown(gen)
  //    startToEnd()
  testAdaptiveShot()


  def warmUp(): Unit = {
    val aql =
      """
        |select `day` as `day`,coll_count(g) as `count`
        |from twitter.ds_tweet t
        |where t.`create_at` >= datetime('2017-01-01T16:23:13.333Z') and t.`create_at` < datetime('2017-01-12T16:23:13.333Z')
        |group by get_day(t.`create_at`) as `day` group as g;
      """.stripMargin
    val f = adbConn.postQuery(aql)
    Await.result(f, scala.concurrent.duration.Duration.Inf)
  }

  def clearCache(): Unit = {
    val aql =
      """
        |
        |select `day` as `day`,coll_count(g) as `count`
        |from twitter.ds_tweet t
        |where t.`create_at` >= datetime('2016-01-01T16:23:13.333Z') and t.`create_at` < datetime('2016-01-12T16:23:13.333Z')
        |group by get_day(t.`create_at`) as `day` group as g;
      """.stripMargin
    val f = adbConn.postQuery(aql)
    Await.result(f, scala.concurrent.duration.Duration.Inf)
  }

  def startToEnd(): Unit = {
    for (keyword <- keywords) {
      val aql = getGroupByDateAndStateAQL(urStartDate, new Duration(urStartDate, urEndDate).getStandardHours.toInt,
        if (keyword.length > 0) Some(keyword) else None)
      val (runTime, avgTime, count) = multipleTime(3, aql)
      println(s"$urStartDate,$urEndDate,$keyword, cold:$runTime, warm: $avgTime, $count")
    }
  }

  def testAdaptiveShot(): Unit = {
    val gaps = Seq(1, 2, 4, 8, 16, 32, 64, 128)
    //    elasticTimeDouble()
    elasticTimeGap()

    /** entry point */
    def elasticTimeGap(): Unit = {

      val terminate = urStartDate
      //        0.9 to 0.1 by -0.1 foreach { discount =>
      //      45 to 2600 by 5 foreach { minGap=>
      //      2 to 3 foreach { runs =>
      val fullHistory = List.newBuilder[QueryStat]
      1 to 3 foreach { runs =>
        Seq(AlgoType.NormalGaussian, AlgoType.Histogram).foreach { algo =>
          val alpha = Math.pow(2, runs - 1)
          val requireTime = 2000

          for (keyword <- keywords) {
            //          for (keyword <- Seq("trump", "clinton", "rain")) {
            val history = List.newBuilder[QueryStat]
            val weight = List.newBuilder[Long]
            weight += 1
            weight += 1

            def streamRun(endTime: DateTime, range: Int, limit: Int, target: Int): Stream[(DateTime, Int)] = {
              val start = endTime.minusHours(range)
              val aql = getCountOnlyAQL(start, range, if (keyword.length > 0) Some(keyword) else None)
              val (runTime, _, count) = multipleTime(0, aql)

              history += QueryStat(target, range, runTime.toInt)
              fullHistory += QueryStat(target, range, runTime.toInt)
              weight += Math.min(weight.result().takeRight(2).sum, Integer.MAX_VALUE)
              println(s"$algo,$alpha,$start,$range,$keyword,$limit,$target,$runTime,$count")

              //            val diff = if (runTime <= limit) limit - runTime else -((runTime - limit) % requireTime)
              val diff = if (runTime <= limit) limit - runTime else 0
              val nextLimit = requireTime + diff.toInt

              val (nextRange, estTarget) = estimateInGeneral(nextLimit, alpha, history.result(), fullHistory.result(), algo)

              (endTime, limit - runTime.toInt) #:: streamRun(start, nextRange.toInt, nextLimit, estTarget.toInt)
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
            println("algorithm,alpha,requireTime,unit,keyword,numQuery,numPenalty,sumPenalty,totalTime,aveTime, $variance")
            println(s"$algo,$alpha,$requireTime,$unit,$keyword,${list.size},${penalty},${sumPenalty / 1000.0},${totalTime / 1000.0},${totalTime / list.size / 1000.0}, $variance")
            println()
          }
        }
      }
    }
  }

  def calcVariance(history: List[QueryStat]): Double = {
    //    history.takeRight(history.size - 3).map(h => (h.targetMS - h.actualMS) * (h.targetMS - h.actualMS)).sum.toDouble / history.size
    //    val underEstimates = history.filter(h => h.targetMS < h.actualMS)
    //    underEstimates.map(h => (h.targetMS - h.actualMS) * (h.targetMS - h.actualMS)).sum.toDouble / underEstimates.size
    val valid = history.filterNot(h => h.targetMS == Int.MaxValue)
    valid.map(h => (h.targetMS - h.actualMS) * (h.targetMS - h.actualMS)).sum.toDouble / valid.size
  }


  def estimateInGeneral(limit: Int, alpha: Double, localHistory: List[QueryStat], globalHistory: List[QueryStat], algoType: AlgoType.Type): (Double, Double) = {
    val lastRange = localHistory.last.estSlice
    val lastTime = localHistory.last.actualMS
    val nextRange = lastRange * limit / lastTime

    def validateRange(range: Double): Double = {
      //      if (1 << localHistory.size < unit) {
      Math.max(1, Math.min(range.toInt, lastRange * 2))
      //      } else {
      //        Math.max(unit, Math.min(range.toInt, lastRange * 4))
      //      }
    }

    //      def validateRange(range: Double): Double = Math.max(1, range)

    val closeRange = Math.max(1, Math.min(nextRange.toInt, lastRange * 2))
    if (localHistory.size < 3) {
      (closeRange, Double.MaxValue)
    } else {
      val variance = calcVariance(globalHistory)
      if (variance < 0.0000001) {
        (closeRange, limit)
      } else {
        val stdDev = Math.sqrt(variance)
        val coeff = trainLinearModel(localHistory)
        algoType match {
          case AlgoType.NormalGaussian =>
            val range = validateRange(Stats.getOptimalRx(timeRange, limit, stdDev, alpha, coeff.a0, coeff.a1))
            println(s"range=$range,limit=$limit, o=$stdDev, a=$alpha, a0=${coeff.a0}, a1=${coeff.a1}")
            (range, range * coeff.a1 + coeff.a0)
          case AlgoType.Histogram =>
            if (globalHistory.size < 10) {
              val rawRange = Stats.getOptimalRx(timeRange, limit, stdDev, alpha, coeff.a0, coeff.a1)
              val range = validateRange(rawRange)
              val rst = (range, range * coeff.a1 + coeff.a0)
              return rst
            }
            val b = 100
            val histo = new Stats.Histogram(b)
            globalHistory.filterNot(_.targetMS == Int.MaxValue).foreach(h => {
              val realDiff = h.actualMS - h.targetMS
              val diff = if (realDiff >= 0) realDiff else -b
              histo += diff
            })

            val rawRange = Stats.getOptimalRx(timeRange, limit, stdDev, alpha, coeff.a0, coeff.a1)
            val rst = (rawRange, rawRange * coeff.a1 + coeff.a0)

            val probs: Seq[Double] = (0 until (limit / b)).map(histo.prob) ++ Seq(histo.cumProb(limit / b))
            val rawRx = Stats.useHistogramUniformFunction(timeRange, limit, b, coeff.a0, coeff.a1, alpha, probs)
            val rx = validateRange(rawRx)
            //            val maxId = exp.zipWithIndex.maxBy(_._1)._2
            //            val target = Math.max(0, limit - (maxId + 1) * b / 2)
            //            val range = validateRange((target - coeff.a0) / coeff.a1)
            val histoIsBig = rawRx > rst._1
            println(s"normal: ${rst._1}, g:${rst._2}; histo: ${rawRx}, g:${rawRx * coeff.a1 + coeff.a0} histoIsBig:$histoIsBig")
            (rx, rx * coeff.a1 + coeff.a0)
          case AlgoType.Baseline =>
            val range = Math.max(1, (limit - coeff.a0) / coeff.a1)
            (range, limit)
        }
      }
    }
  }

  def trainLinearModel(history: List[QueryStat]): Stats.Coeff = {
    val obs: WeightedObservedPoints = new WeightedObservedPoints()
    if (history.size > 3) {
      history.takeRight(history.size - 3).foreach(h => obs.add(h.estSlice, h.actualMS))
    } else {
      history.foreach(h => obs.add(h.estSlice, h.actualMS))
    }
    //      history.zip(weight).foreach{ case (h,w) =>
    //        obs.add(0.5/sumWeight, h.estSlice, h.actualMS)
    //      }

    //      if (history.size <= 10) {
    //        history.foreach { h =>
    //          obs.add(0.5, history.last.estSlice, history.last.actualMS)
    //        }
    //      } else {
    //        val sumWeight = history.size - 10
    //        history.take(history.size - 10).foreach { h =>
    //          obs.add(0.1 / sumWeight, h.estSlice, h.actualMS)
    //        }
    //        obs.add(0.45, history(history.size-2).estSlice, history(history.size-2).actualMS)
    //        obs.add(0.45, history.last.estSlice, history.last.actualMS)
    //      }

    val rawCoeff = Stats.linearFitting(obs)

    if (rawCoeff.a0 <= Double.MinPositiveValue || rawCoeff.a1 <= Double.MinPositiveValue) {
      Stats.Coeff(Double.MinPositiveValue, history.last.actualMS.toDouble / history.last.estSlice)
    } else {
      rawCoeff
    }
  }

  //TODO change to multiple keywords
  def getGroupByDateAndStateAQL(start: DateTime, rangeInHour: Int, keyword: Option[String]): String = {
    val keywordFilter = keyword.map(k => FilterStatement(TextField("text"), None, Relation.contains, Seq(k)))
    val timeFilter = FilterStatement(TimeField("create_at"), None, Relation.inRange,
      Seq(TimeField.TimeFormat.print(start),
        TimeField.TimeFormat.print(start.plusHours(rangeInHour))))
    val filters = keywordFilter.map(Seq(timeFilter, _)).getOrElse(Seq(timeFilter))
    val byDay = ByStatement(TimeField("create_at"), Some(Interval(TimeUnit.Hour, 4)), Some(NumberField("day")))
    val byState = ByStatement(NumberField("geo_tag.stateID"), None, Some(NumberField("state")))
    val groupStatement = GroupStatement(Seq(byDay, byState), Seq(aggrCount))
    //    val groupStatement = GroupStatement(Seq(byDay), Seq(aggrCount))
    val query = Query(dataset = "twitter.ds_tweet", filter = filters, groups = Some(groupStatement))
    //    val query = Query(dataset = "twitter.ds_tweet", filter = filters, globalAggr = Some(globalAggr))
    queryGen.generate(query, Map(TwitterDataStore.DatasetName -> TwitterDataStore.TwitterSchema))
  }

  def getCountOnlyAQL(start: DateTime, rangeInHour: Int, keyword: Option[String]): String = {
//    val name = "twitter.ds_tweet_prefix"
    val name = "twitter.ds_tweet"
    val keywordFilter = keyword.map(k => FilterStatement(TextField("text"), None, Relation.contains, Seq(k)))
    val timeFilter = FilterStatement(TimeField("create_at"), None, Relation.inRange,
      Seq(TimeField.TimeFormat.print(start),
        TimeField.TimeFormat.print(start.plusHours(rangeInHour))))
    val filters = keywordFilter.map(Seq(timeFilter, _)).getOrElse(Seq(timeFilter))
    val byDay = ByStatement(TimeField("create_at"), Some(Interval(TimeUnit.Day)), Some(NumberField("day")))
    //    val groupStatement = GroupStatement(Seq(byDay), Seq(aggrCount))
    //    val groupStatement = GroupStatement(Seq(byDay), Seq(aggrCount))
    //    val query = Query(dataset = "twitter.ds_tweet", filter = filters, globalAggr = Some(globalAggr))
    val query = Query(dataset = name, filter = filters, globalAggr = Some(globalAggr))
    //        val query = Query(dataset = "twitter.ds_tweet", filter = filters, groups = Some(groupStatement))
    //    val query = Query(dataset = "twitter.ds_tweet_prefix", filter = filters, groups = Some(groupStatement))
    queryGen.generate(query, Map(name -> TwitterDataStore.TwitterSchema))
  }

  exit()
}



