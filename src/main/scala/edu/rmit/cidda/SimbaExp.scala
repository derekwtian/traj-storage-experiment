package edu.rmit.cidda

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.simba.SimbaSession
import edu.rmit.cidda.utils.DateTime
import org.apache.spark.sql.simba.index.RTreeType


object SimbaExp {
  case class PointData(id: String, x: Double, y: Double, t: Double)

  val id = "538004086"
  val startTime = "2019-02-01T00:00:00"
  val tqueries = List("2019-02-01T00:59:59", "2019-02-01T05:59:59", "2019-02-01T23:59:59", "2019-02-07T23:59:59", "2019-02-28T23:59:59")
  val queries = List(
    (-118.28, -118.23, 33.68, 33.73),
    (-118.30, -118.20, 33.66, 33.76),
    (-118.35, -118.15, 33.61, 33.81),
    (-118.45, -118.05, 33.51, 33.91),
    (-118.50, -118.00, 33.46, 33.96)
  )

  var count = 0L
  var eachQueryLoopTimes = 5

  def main(args: Array[String]): Unit = {
    var master = "local[4]"
    var PartitionsNum = 1024
    var PointInputPath = "file:///Users/tianwei/Projects/data/ais_small.csv"

    if (args.length > 0) {
      if (args.length % 2 == 0) {
        var i = 0
        while (i < args.length) {
          args(i) match {
            case "m" => master = args(i+1)
            case "f" => PointInputPath = args(i+1)
            case "p" => PartitionsNum = args(i+1).toInt
            case "l" => eachQueryLoopTimes = args(i+1).toInt
          }
          i += 2
        }
      } else {
        println("Please check parameters!")
        return
      }
    }
    println(s"Parameters: $master, $PointInputPath, $PartitionsNum, $eachQueryLoopTimes")

    val simbaSession = SimbaSession.builder().appName("SimbaAISExperiment").config("simba.index.partitions", PartitionsNum.toString).master(master).getOrCreate()
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    import simbaSession.implicits._
    import simbaSession.simbaImplicits._

    println(s"Input Data File: $PointInputPath")

    var start = System.currentTimeMillis()
    val leftDF = simbaSession.read.textFile(PointInputPath).map(line => {
      val items = line.split(",")
      PointData(items(0), items(3).toDouble, items(2).toDouble, items(1).toLong * 1000)
    }).repartition(PartitionsNum).toDF()
    count = leftDF.count()
    println(s"<Simba> Generate DataFrame, use time ${System.currentTimeMillis() - start}ms, $count")


    start = System.currentTimeMillis()
    leftDF.createOrReplaceTempView("point1")
    println(s"<Simba> Table Register, use time ${System.currentTimeMillis() - start}ms")
    simbaSession.indexTable("point1", RTreeType, "RTree", Array("x", "y"))

    //dry run
    for (i <- 1 to 20) {
      val count1 = simbaSession.sql("SELECT * FROM point1 WHERE x >= -180 and x <= 180 and y >= -90 and y <= 90").rdd.count()
    }

    testIDTQuery(simbaSession)
    testSRQuery(simbaSession)
    testSTQuery(simbaSession)

//    val df = simbaContext.sql("SELECT * FROM point1 WHERE x <= -118.23 and x >= -118.28 and y >= 33.68 and y <= 33.73 and id == '538004086'")//.collect().foreach(println)
//    df.explain(true)
//    df.show()

    /*leftDF.index(RTreeType, "rt", Array("x", "y"))

    val df = leftDF.knn(Array("x", "y"), Array(10.0, 10), 3)
    println(df.queryExecution)
    df.show()

    leftDF.range(Array("x", "y"), Array(-118.28, -118.23), Array(33.68, 33.73)).show()

    leftDF.knnJoin(rightDF, Array("x", "y"), Array("x", "y"), 3).show(100)*/

    simbaSession.stop()
    println("All Simba Measurements finished!")
  }

  def testIDTQuery(simba: SimbaSession): Unit = {
    simba.clearIndex()
    println("=========Test ID-Temporal Query=========")
    var start = System.currentTimeMillis()
    simba.indexTable("point1", RTreeType, "idt", Array("t"))
    println(s"<Simba> Build IDT Index, use time ${System.currentTimeMillis() - start}ms")
    println(simba.showIndex("point1"))

    tqueries.foreach(query => {
      println(query)
      val sql = "SELECT * FROM point1 WHERE t>=" + DateTime.format2Stamp(startTime) + " and t<=" + DateTime.format2Stamp(query) + " and id='" + id + "'"
      println(s"ID-Temporal Range: $sql")

      var resultSize = 0L
      val t0 = System.currentTimeMillis()
      for (i <- 1 to eachQueryLoopTimes) {
        resultSize = simba.sql(sql).rdd.count()
      }
      val t1 = System.currentTimeMillis()

      println("Selection Ratio (%): " + ((resultSize * 100.0) / count))
      println(s"<Simba> ID-Temporal Query finished, use time ${(t1 - t0) / eachQueryLoopTimes}ms, $resultSize")
    })
  }

  def testSRQuery(simba: SimbaSession): Unit = {
    simba.clearIndex()
    println("=========Test Spatial Range Query=========")
    var start = System.currentTimeMillis()
    simba.indexTable("point1", RTreeType, "rt", Array("x", "y"))
    println(s"<Simba> Build RT Index, use time ${System.currentTimeMillis() - start}ms")
    println(simba.showIndex("point1"))

    queries.foreach(query => {
      println(query)
      val sql = "SELECT * FROM point1 WHERE x>=" + query._1 + " and x<=" + query._2 + " and y>=" + query._3 + " and y<=" + query._4
      println(s"Spatial Range: $sql")

      var resultSize = 0L
      val t0 = System.currentTimeMillis()
      for (i <- 1 to eachQueryLoopTimes) {
        resultSize = simba.sql(sql).rdd.count()
      }
      val t1 = System.currentTimeMillis()

      println("Selection Ratio (%): " + ((resultSize * 100.0) / count))
      println(s"<Simba> Spatial Range Query finished, use time ${(t1 - t0) / eachQueryLoopTimes}ms, $resultSize")
    })
  }

  def testSTQuery(simba: SimbaSession): Unit = {
    simba.clearIndex()
    println("=========Test Spatial-Temporal Range Query=========")
    var start = System.currentTimeMillis()
    simba.indexTable("point1", RTreeType, "st", Array("x", "y", "t"))
    println(s"<Simba> Build ST Index, use time ${System.currentTimeMillis() - start}ms")
    println(simba.showIndex("point1"))

    tqueries.foreach(tquery => {
      queries.foreach(query => {
        println(tquery, query)
        val sql = "SELECT * FROM point1 WHERE x>=" + query._1 + " and x<=" + query._2 + " and y>=" + query._3 + " and y<=" + query._4 + " and t>=" + DateTime.format2Stamp(startTime) + " and t<=" + DateTime.format2Stamp(tquery)
        println(s"Spatial Range: $sql")

        var resultSize = 0L
        val t0 = System.currentTimeMillis()
        for (i <- 1 to eachQueryLoopTimes) {
          resultSize = simba.sql(sql).rdd.count()
        }
        val t1 = System.currentTimeMillis()

        println("Selection Ratio (%): " + ((resultSize * 100.0) / count))
        println(s"<Simba> Spatial-Temporal Range Query finished, use time ${(t1 - t0) / eachQueryLoopTimes}ms, $resultSize")
      })
    })
  }
}
