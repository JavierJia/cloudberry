package edu.uci.ics.cloudberry.zion.experiment

import java.io.File
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigFactory
import edu.uci.ics.cloudberry.zion.model.datastore.AsterixConn
import edu.uci.ics.cloudberry.zion.model.impl.{AQLGenerator, TwitterDataStore}
import edu.uci.ics.cloudberry.zion.model.schema._
import org.asynchttpclient.AsyncHttpClientConfig
import org.joda.time.{DateTime, Duration}
import play.api.libs.ws.WSConfigParser
import play.api.libs.ws.ahc.{AhcConfigBuilder, AhcWSClient, AhcWSClientConfig}
import play.api.{Configuration, Environment, Mode}

import scala.concurrent.{Await, ExecutionContext, Future}

object ResponseTime extends App {
  val gen = new AQLGenerator()
  val aggrCount = AggregateStatement("*", Count, "count")
  val globalAggr = GlobalAggregateStatement(aggrCount)

  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(2))
  implicit val system = ActorSystem()
  implicit val mat = ActorMaterializer()
  val wsClient = produceWSClient()
  val url = "http://actinium.ics.uci.edu:19002/aql"
  val asterixConn = new AsterixConn(url, wsClient)

  warmUp()
  testFirstShot()

  def exit(): Unit = {
    wsClient.close()
    System.exit(0)
  }

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
    val config = new AhcWSClientConfig(wsClientConfig = parser.parse())
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

  def warmUp(): Unit = {
    val aql =
      """
        |for $d in dataset twitter.ds_tweet
        |where $d.create_at >= datetime('2016-08-01T08:00:00.000Z') and
        |      $d.create_at <= datetime('2016-08-02T08:00:00.000Z')
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

  def testFirstShot(): Unit = {
    val gaps = Seq(1, 2, 4, 8, 16, 32, 64, 128)
    //    val keywords = Seq("happy", "zika", "uci", "trump", "a")
    val keywords = Seq("zika", "pitbull", "goal", "bro", "happy")
    //    keywordWithTime()
    //    selectivity(keywords)
    //      keywordWithContinueTime()
    //        elasticTimeGap()
    // elasticAdaptiveGap()
    //    testOverheadOfMultipleQueries()
//    testSamplingPerf()
    elasticAdaptiveGap()


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
          val aql = getAQL(now.minusHours(gap), gap, keyword)

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

            val aql = getAQL(start.minusHours(gap), gap, keyword)
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

    def elasticTimeGap(): Unit = {
      val repeat = 15
      val requireTime = 2000
      Seq(1.0, 0.9, 0.8, 0.7, 0.6, 0.5).foreach { lambda =>
        for (keyword <- keywords) {
          var start = DateTime.now()
          var gap = 2
          var (historyGap, historyTime) = (0, 1l)
          1 to repeat foreach { i =>

            val aql = getAQL(start.minusHours(gap), gap, keyword)
            val (lastTime, avg, count) = multipleTime(0, aql)

            println(s"$gap,$keyword,$lastTime,$count")

            start = start.minusHours(gap)
            val newGap = Math.max(formular(requireTime, 1.0,  gap, lastTime, historyGap, historyTime, lambda), 1)
            historyGap += gap
            historyTime += lastTime
            gap = newGap
          }
        }
      }
    }

    def elasticAdaptiveGap(): Unit = {
      val ExpectGap = 3000
//      1 to 3 foreach { n =>
      1 to 1 foreach { n =>
        val reportGap = ExpectGap
        for (keyword <- keywords) {
          var start = DateTime.now()
          val end = start.minusDays(90)
          var gap = 2
          var lastExpectTime = ExpectGap
          var (historyGap, historyTime) = (0, 1l)
          val batchStart = DateTime.now()
          var times = 0
          var (checkPoint, numResults) = (DateTime.now().plusMillis(ExpectGap), 0)
          while (start.minusHours(gap).getMillis >= end.getMillis) {
            val aql = getAQL(start.minusHours(gap), gap, keyword)
            val (lastTime, _, count) = multipleTime(0, aql)

            val now = DateTime.now
            var missed = now.getMillis - checkPoint.getMillis
            if (missed < 200) {
              numResults += 1
            } else if (numResults < 1) {
              println(s"${now.getMillis},${checkPoint.getMillis},$keyword,missed,$missed")
            }
            while (missed > 0) {
              checkPoint = checkPoint.plusMillis(ExpectGap)
              numResults = 0
              missed = now.getMillis - checkPoint.getMillis
              if (missed < 200) {
                numResults += 1
              } else if (numResults < 1) {
                println(s"${now.getMillis},${checkPoint.getMillis},$keyword,missed,$missed")
              }
            }

            println(s"${now.getMillis},${checkPoint.getMillis},$gap,$keyword,$lastTime,$lastExpectTime,$count")

            start = start.minusHours(gap)
            lastExpectTime = Math.max(reportGap + (lastExpectTime - lastTime), 1).toInt

            val newGap = Math.max(formular(lastExpectTime, 0.7, gap, lastTime, historyGap, historyTime, 1.0), 1)
            historyGap += gap
            historyTime += lastTime
            gap = newGap
            times += 1
          }
          if (start.minusHours(gap).getMillis < end.getMillis && start.getMillis > end.getMillis) {
            val aql = getAQL(end, new Duration(end, start).getStandardHours.toInt, keyword)
            val (lastRunTime, avg, count) = multipleTime(0, aql)

            println(s"${DateTime.now.getMillis},${checkPoint.getMillis},$gap,$keyword,$lastRunTime,$lastExpectTime,$count")
            times += 1
          }
          println(s"$times,$keyword,${(DateTime.now.getMillis - batchStart.getMillis) / 1000}")
          println()
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
            val aql = getAQL(startTime, g, keyword)
            val (lastTime, _, count) = multipleTime(0, aql)
            println(s"$keyword,$startTime,$g,$lastTime,$count")
          }
          println(s"$keyword,$slices,${DateTime.now().getMillis - begin.getMillis}")
          println()
        }
      }
    }

    def formular(requireTime: Int, risk : Double, lastGap: Int, lastTime: Long, histoGap: Int, histoTime: Long, lambda: Double): Int = {
      lambda * lastGap * requireTime * risk / lastTime +
        (1 - lambda) * requireTime * histoGap * 1.0 / histoTime toInt
    }
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
    (firstTime, sum * 1.0 / (times + 0.001), count)
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
        val aql = getAQL(start, gap, "happy")
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


  def getAQL(start: DateTime, gapHour: Int, keyword: String): String = {
    val keywordFilter = FilterStatement("text", None, Relation.contains, Seq(keyword))
    val timeFilter = FilterStatement("create_at", None, Relation.inRange,
      Seq(TimeField.TimeFormat.print(start),
        TimeField.TimeFormat.print(start.plusHours(gapHour))))
    val byHour = ByStatement("create_at", Some(Interval(TimeUnit.Minute, 10 * gapHour)), Some("hour"))
    val groupStatement = GroupStatement(Seq(byHour), Seq(aggrCount))
    //      val query = Query(dataset = "twitter.ds_tweet", filter = Seq(timeFilter), groups = Some(groupStatement))
    val query = Query(dataset = "twitter.ds_tweet", filter = Seq(timeFilter, keywordFilter), globalAggr = Some(globalAggr))
    gen.generate(query, TwitterDataStore.TwitterSchema)
  }

  def getSeqTimeAQL(timeSeq: Seq[(DateTime, DateTime)], keyword: String): Seq[String] = {
    val keywordFilter = FilterStatement("text", None, Relation.contains, Seq(keyword))
    val timeFilters = timeSeq.map { case (start, end) =>
      FilterStatement("create_at", None, Relation.inRange, Seq(TimeField.TimeFormat.print(start), TimeField.TimeFormat.print(end)))
    }
    timeFilters.map { f =>
      val query = Query(dataset = "twitter.ds_tweet", filter = Seq(keywordFilter, f), globalAggr = Some(globalAggr))
      gen.generate(query, TwitterDataStore.TwitterSchema)
    }
  }

  def getCountKeyword(keyword: String): String = {
    val keywordFilter = FilterStatement("text", None, Relation.contains, Seq(keyword))
    val query = Query(dataset = "twitter.ds_tweet", filter = Seq(keywordFilter), globalAggr = Some(globalAggr))
    gen.generate(query, TwitterDataStore.TwitterSchema)
  }

  def getCountTime(start: DateTime, end: DateTime): String = {
    val timeFilter = FilterStatement("create_at", None, Relation.inRange,
      Seq(TimeField.TimeFormat.print(start),
        TimeField.TimeFormat.print(end)))
    val query = Query(dataset = "twitter.ds_tweet", filter = Seq(timeFilter), globalAggr = Some(globalAggr))
    gen.generate(query, TwitterDataStore.TwitterSchema)
  }

  exit()
}
