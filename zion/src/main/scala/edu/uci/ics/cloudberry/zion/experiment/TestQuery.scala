package edu.uci.ics.cloudberry.zion.experiment

import edu.uci.ics.cloudberry.zion.experiment.ResponseTime.{adbConn, queryGen, multipleTime, urEndDate, urStartDate}
import edu.uci.ics.cloudberry.zion.model.impl.{AQLGenerator}
import edu.uci.ics.cloudberry.zion.model.schema.TimeField.TimeFormat
import edu.uci.ics.cloudberry.zion.model.schema._
import org.joda.time.{DateTime, Duration}

import scala.concurrent.Await

/**
  * Created by jianfeng on 4/16/17.
  */
class TestQuery extends Connection {

  def getSeqTimeAQL(timeSeq: Seq[(DateTime, DateTime)], keyword: String): Seq[String] = {
    val keywordFilter = FilterStatement(TextField("text"), None, Relation.contains, Seq(keyword))
    val timeFilters = timeSeq.map { case (start, end) =>
      FilterStatement(TimeField("create_at"), None, Relation.inRange, Seq(TimeField.TimeFormat.print(start), TimeField.TimeFormat.print(end)))
    }
    timeFilters.map { f =>
      val query = Query(dataset = "twitter.ds_tweet", filter = Seq(keywordFilter, f), globalAggr = Some(globalAggr))
      queryGen.generate(query, Map(TwitterDataStore.DatasetName -> TwitterDataStore.TwitterSchema))
    }
  }

  def getCountKeyword(keyword: String): String = {
    val keywordFilter = FilterStatement(TextField("text"), None, Relation.contains, Seq(keyword))
    val query = Query(dataset = "twitter.ds_tweet", filter = Seq(keywordFilter), globalAggr = Some(globalAggr))
    queryGen.generate(query, Map(TwitterDataStore.DatasetName -> TwitterDataStore.TwitterSchema))
  }

  def getCountTime(start: DateTime, end: DateTime): String = {
    val timeFilter = FilterStatement(TimeField("create_at"), None, Relation.inRange,
      Seq(TimeField.TimeFormat.print(start),
        TimeField.TimeFormat.print(end)))
    val query = Query(dataset = "twitter.ds_tweet", filter = Seq(timeFilter), globalAggr = Some(globalAggr))
    queryGen.generate(query, Map(TwitterDataStore.DatasetName -> TwitterDataStore.TwitterSchema))
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
    queryGen.generate(query, Map(TwitterDataStore.DatasetName -> TwitterDataStore.TwitterSchema))
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
      val f = adbConn.postQuery(aql).map { ret =>
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


}

object GetSampleID extends App {
  val gen = new AQLGenerator()
  val start = new DateTime(2016, 1, 1, 0, 0)

  def sampleQuery(start: DateTime): String = {

    s"""
       |for $$d in dataset twitter.ds_tweet
       |  where $$d.create_at >= datetime("${TimeFormat.print(start)}")
       |  and $$d.create_at < datetime("${TimeFormat.print(start.plusDays(1))}")
       |  limit 100 return $$d
     """.stripMargin
  }

  def stream(start: DateTime): Stream[DateTime] = start #:: stream(start.plusDays(1))

  stream(start).takeWhile(now => now.getMillis < DateTime.now().getMillis).foreach(time =>
    println(sampleQuery(time))
  )
}

