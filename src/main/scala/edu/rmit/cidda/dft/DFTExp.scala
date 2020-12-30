package edu.rmit.cidda.dft

import edu.utah.cs.spatial.{LineSegment, Point}
import edu.utah.cs.trajectory.TrajMeta
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object DFTExp {
  var sc: SparkContext = null

  def main(args: Array[String]): Unit = {
    var threshold_values = Array(0.05, 0.1, 0.2, 0.3, 0.4, 0.5)
    var k_values = Array(1, 10, 30, 50, 70, 100)
    val c = 5
    var printer = false

    var idOffset = 3
    var startOffset = 5
    val endOffset = startOffset + Array("x", "y", "t").length

    var simFunc = "F"
    var eachQueryLoopTimes = 5
    var master = "local[*]"
    var mrs = "4g"
    var query_traj_filename = "file:///Users/tianwei/Projects/data/DFT_format/query/queries_dft.csv"
    var traj_data_filename = "file:///Users/tianwei/Projects/data/DFT_format/v25_dft.csv"

    if (args.length > 0) {
      if (args.length % 2 == 0) {
        var i = 0
        while (i < args.length) {
          args(i) match {
            case "m" => master = args(i+1)
            case "mrs" => mrs = args(i+1)
            case "q" => query_traj_filename = args(i+1)
            case "t" => traj_data_filename = args(i+1)
            case "id" => idOffset = args(i+1).toInt
            case "start" => startOffset = args(i+1).toInt
            case "df" => simFunc = args(i+1)
            case "l" => eachQueryLoopTimes = args(i+1).toInt
            case "x" => if (args(i+1).equals("scale")) {
              k_values = Array(50)
              threshold_values = Array(0.2)
            }
            case "s" => printer = args(i+1).toBoolean
          }
          i += 2
        }
      } else {
        println("usage: Please check parameters!")
        System.exit(1)
      }
    }
    println(s"Parameters: $master, $mrs, $query_traj_filename, $traj_data_filename, $eachQueryLoopTimes, $printer")

    val conf = new SparkConf().setAppName("DFT")
      .set("spark.locality.wait", "0")
      .set("spark.driver.maxResultSize", mrs)
    if (!master.equals("dla")) {
      conf.setMaster(master)
    }
    sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)


    println(s"Input Data File: $query_traj_filename, $traj_data_filename, $simFunc, $idOffset, $startOffset, $endOffset")

    val queryTraj = sc.textFile(query_traj_filename)
    val queryIDMap = queryTraj.map(line => {
      val splitted = line.split(',')
      (splitted(idOffset).toInt, splitted(0))
    }).collect().toMap

    val queries = queryTraj.map { line =>
      val splitted = line.split(',')
      (splitted(idOffset).toInt, LineSegment(
        Point(Array(splitted(startOffset).toDouble, splitted(startOffset + 1).toDouble)),
        Point(Array(splitted(endOffset).toDouble, splitted(endOffset + 1).toDouble))))
    }.collect().groupBy(_._1).map(x => (x._1, x._2.map(_._2))).toArray.slice(0, 100)


    val start = System.currentTimeMillis()

    val trajBase = sc.textFile(traj_data_filename)
    val idMap = trajBase.map(x => {
      val splitted = x.split(",")
      (splitted(idOffset).toInt, splitted(0))
    }).collect().toMap

    val dataRDD = trajBase.map(x => x.split(',')).map(x => (LineSegment(
        Point(Array(x(startOffset).toDouble, x(startOffset + 1).toDouble)),
        Point(Array(x(endOffset).toDouble, x(endOffset + 1).toDouble))),
        TrajMeta(x(idOffset).toInt, x(idOffset + 1).toInt)))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val trajs = trajBase.mapPartitions(iter => {
      iter.map(x => {
        val splitted = x.split(",")
        (splitted(idOffset).toInt, LineSegment(
          Point(Array(splitted(startOffset).toDouble, splitted(startOffset + 1).toDouble)),
          Point(Array(splitted(endOffset).toDouble, splitted(endOffset + 1).toDouble))))
      }).toArray.groupBy(_._1).map(now => {
        val cur_traj = now._2.sortBy(_._1).map(_._2)
        (DFT.getMBR(cur_traj), (now._1, cur_traj))
      }).iterator
    })
      .persist(StorageLevel.MEMORY_AND_DISK)

    DFT.buildIndex(dataRDD, trajs)
    dataRDD.unpersist()
    trajs.unpersist()

    println("------------------------------------------------------------")
    println(s"<DFT> Time to build indexes: ${System.currentTimeMillis() - start}ms")
    println("------------------------------------------------------------")

    println("==> kNN Search")
    k_values.foreach(k => {
      var tot_time = 0.0
      queries.foreach(item => {
        val query_traj = item._2
        println(s"---------kNN trajectory query: TID ${item._1}, ${query_traj.length}----------")

        var res: Array[(Double, Int)] = null
        var sum = 0L
        for (i <- 1 to eachQueryLoopTimes) {
          println(s"---------looptime: $i----------")
          val t0 = System.currentTimeMillis()
          res = kNNSearch(query_traj, simFunc, k, c)
          val t1 = System.currentTimeMillis()

          sum += t1 - t0

          res.map(item => {
            (idMap(item._2), item._1)
          }).foreach(println)
        }

        println(s"<DFT-k> Total Latency (k = $k): ${queryIDMap(item._1)} -> ${sum / eachQueryLoopTimes.toDouble}ms, ${res.length}")

        if (printer) {
          println("The results show as below:")
          DFT.refetchTraj(res).map(item => {
            (item._1, item._2, item._3.mkString(", "))
          }).foreach(println)
        }
        println("------------------------------------------------------------")

        tot_time += sum / eachQueryLoopTimes.toDouble
      })

      println(s"<DFTkNN> Average Latency for k = $k is : ${tot_time / queries.length}ms")
      println("===================================================")
    })

    println("==> Threshold Search")
    threshold_values.foreach(threshold => {
      var tot_time = 0.0
      queries.foreach(item => {
        val query_traj = item._2
        println(s"---------similarity query: TID ${item._1}, ${query_traj.length}----------")

        var res: Array[(Double, Int)] = null
        var sum = 0L
        for (i <- 1 to eachQueryLoopTimes) {
          println(s"---------looptime: $i----------")
          val t0 = System.currentTimeMillis()
          res = thresholdSearch(query_traj, simFunc, threshold)
          val t1 = System.currentTimeMillis()

          sum += t1 - t0

          res.sorted(new ResultOrdering).map(item => {
            (idMap(item._2), item._1)
          }).foreach(println)
        }

        println(s"<DFT-s> Total Latency (threshold = $threshold): ${queryIDMap(item._1)} -> ${sum / eachQueryLoopTimes.toDouble}ms, ${res.length}")

        if (printer) {
          println("The results show as below:")
          DFT.refetchTraj(res).map(item => {
            (item._1, item._2, item._3.mkString(", "))
          }).foreach(println)
        }
        println("------------------------------------------------------------")

        tot_time += sum / eachQueryLoopTimes.toDouble
      })

      println(s"<DFTSim> Average Latency for threshold = $threshold is : ${tot_time / queries.length}ms")
      println("===================================================")
    })

    sc.stop()
    println("All DFT Measurements finished!")
  }

  def kNNSearch(query_traj: Array[LineSegment], simFunc: String, k: Int, c:Int): Array[(Double, Int)] = {
    var start = System.currentTimeMillis()

    val bc_query = sc.broadcast(query_traj)
    val pruning_bound = DFT.calcPruningBound(bc_query.value, simFunc, k, c, sc)

    println(s"==> Time to calculate pruning bound: ${System.currentTimeMillis() - start}ms")
    println("The pruning bound is: " + pruning_bound)

    start = System.currentTimeMillis()
    val res = DFT.candiSelection(bc_query.value, simFunc, pruning_bound, sc)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    //bc_query.destroy()
    println(s"==> Time to finish the final filter: ${System.currentTimeMillis() - start}ms")
    //println(s"# of distance calculated: ${c * k + res.count()}")

    start = System.currentTimeMillis()
    val result = res.takeOrdered(k)(new ResultOrdering)
    println(s"==> Time to get top-$k results: ${System.currentTimeMillis() - start}ms")
    res.unpersist()

    result
  }

  def thresholdSearch(query_traj: Array[LineSegment], simFunc: String, threshold: Double): Array[(Double, Int)] = {
    var start = System.currentTimeMillis()

    val bc_query = sc.broadcast(query_traj)
    val res = DFT.candiSelection(bc_query.value, simFunc, threshold, sc)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    //bc_query.destroy()

    println(s"==> Time to finish the final filter: ${System.currentTimeMillis() - start}ms")
    //println(s"# of distance calculated: ${res.count()}")

    start = System.currentTimeMillis()
    val result = res.filter(item => {
      item._1 <= threshold
    }).collect()
    println(s"==> Time to get results less than $threshold: ${System.currentTimeMillis() - start}ms")
    res.unpersist()

    result
  }

  private class ResultOrdering extends Ordering[(Double, Int)] {
    override def compare(x: (Double, Int), y: (Double, Int)): Int = x._1.compare(y._1)
  }

}
