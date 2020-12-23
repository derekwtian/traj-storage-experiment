package edu.rmit.cidda.dft

import edu.utah.cs.index.RTree
import edu.utah.cs.index_rr.RTreeWithRR
import edu.utah.cs.spatial.{LineSegment, MBR, Point}
import edu.utah.cs.trajectory.TrajMeta
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap


object DFTExp {
  var sc: SparkContext = null

  var compressed_traj: RDD[(Int, Array[Byte])] = null
  var traj_global_rtree: RTree = null
  var indexed_seg_rdd: RDD[RTreeWithRR] = null
  var stat: Array[(MBR, Long, RoaringBitmap)] = null
  var global_rtree: RTree = null

  def main(args: Array[String]): Unit = {
    var threshold_values = Array(0.05, 0.1, 0.2, 0.3, 0.4, 0.5)
    var k_values = Array(1, 10, 30, 50, 70, 100)
    val c = 5
    var printer = false

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
            case "b" => traj_data_filename = args(i+1)
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


    println(s"Input Data File: $query_traj_filename, $traj_data_filename")

    val queries = sc.textFile(query_traj_filename).map { line =>
      val splitted = line.split(',')
      (splitted(3).toInt, LineSegment(Point(Array(splitted(5).toDouble, splitted(6).toDouble)),
        Point(Array(splitted(8).toDouble, splitted(9).toDouble))))
    }.collect().groupBy(_._1).map(x => (x._1, x._2.map(_._2)))


    val start = System.currentTimeMillis()

    val dataRDD = sc.textFile(traj_data_filename)
      .map(x => x.split(','))
      .map(x => (LineSegment(Point(Array(x(5).toDouble, x(6).toDouble)),
        Point(Array(x(8).toDouble, x(9).toDouble))),
        TrajMeta(x(3).toInt, x(4).toInt)))

    val trajs = sc.textFile(traj_data_filename).mapPartitions(iter => {
      iter.map(x => {
        val splitted = x.split(",")
        (splitted(3).toInt,
          LineSegment(Point(Array(splitted(5).toDouble, splitted(6).toDouble)),
            Point(Array(splitted(8).toDouble, splitted(9).toDouble))))
      }).toArray.groupBy(_._1).map(now => {
        val cur_traj = now._2.sortBy(_._1).map(_._2)
        (DFT.getMBR(cur_traj), (now._1, cur_traj))
      }).iterator
    })

    val index = DFT.buildIndex(dataRDD, trajs)
    compressed_traj = index._1
    traj_global_rtree = index._2
    indexed_seg_rdd = index._3
    stat = index._4
    global_rtree = index._5

    println("------------------------------------------------------------")
    println(s"<DFT> Time to build indexes: ${System.currentTimeMillis() - start}ms")
    println("------------------------------------------------------------")

    println("==> kNN Search")
    k_values.foreach(k => {
      var tot_time = 0.0
      queries.foreach(item => {
        val query_traj = item._2
        var res: Array[(Double, Int, Array[Byte])] = null
        val t0 = System.currentTimeMillis()
        for (i <- 1 to eachQueryLoopTimes) {
          println(s"---------looptime: ${i}----------")
          res = kNNSearch(query_traj, k, c)
        }
        val t1 = System.currentTimeMillis()
        tot_time += (t1 - t0) / eachQueryLoopTimes.toDouble

        println(s"---------kNN trajectory query: TID ${item._1}, ${query_traj.length}----------")
        println(s"<DFT-k> Total Latency: ${(t1 - t0) / eachQueryLoopTimes.toDouble}ms, ${res.length}")

        if (printer) {
          println("The results show as below:")
          res.map(item => {
            val traj = DFT.trajReconstruct(item._3)
            val points = traj.map(item => item.start) :+ traj.last.end
            (item._2, item._1, points.mkString(", "))
          }).foreach(println)
        }
        println("------------------------------------------------------------")
      })

      println(s"<DFTkNN> Average Latency for k = $k is : ${tot_time / queries.size}ms")
      println("===================================================")
    })

    println("==> Threshold Search")
    threshold_values.foreach(threshold => {
      var tot_time = 0.0
      queries.foreach(item => {
        val query_traj = item._2
        var res: Array[(Double, Int, Array[Byte])] = null
        val t0 = System.currentTimeMillis()
        for (i <- 1 to eachQueryLoopTimes) {
          println(s"---------looptime: ${i}----------")
          res = thresholdSearch(query_traj, threshold)
        }
        val t1 = System.currentTimeMillis()
        tot_time += (t1 - t0) / eachQueryLoopTimes.toDouble

        println(s"---------similarity query: TID ${item._1}, ${query_traj.length}----------")
        println(s"<DFT-s> Total Latency: ${(t1 - t0) / eachQueryLoopTimes.toDouble}ms, ${res.length}")

        if (printer) {
          println("The results show as below:")
          res.map(item => {
            val traj = DFT.trajReconstruct(item._3)
            val points = traj.map(item => item.start) :+ traj.last.end
            (item._2, item._1, points.mkString(", "))
          }).foreach(println)
        }
        println("------------------------------------------------------------")
      })

      println(s"<DFTThreshold> Average Latency for threshold = $threshold is : ${tot_time / queries.size}ms")
      println("===================================================")
    })

    sc.stop()
    println("All DFT Measurements finished!")
  }

  def kNNSearch(query_traj: Array[LineSegment], k: Int, c:Int): Array[(Double, Int, Array[Byte])] = {
    var start = System.currentTimeMillis()

    val bc_query = sc.broadcast(query_traj)
    val pruning_bound = DFT.calcPruningBound(bc_query.value, k, c, sc, compressed_traj, global_rtree, stat, traj_global_rtree)

    println(s"==> Time to calculate pruning bound: ${System.currentTimeMillis() - start}ms")
    println("The pruning bound is: " + pruning_bound)

    start = System.currentTimeMillis()
    val res = DFT.candiSelection(bc_query.value, pruning_bound, sc, compressed_traj, global_rtree, stat, traj_global_rtree, indexed_seg_rdd).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //bc_query.destroy()
    println(s"==> Time to finish the final filter: ${System.currentTimeMillis() - start}ms")
    println(s"# of distance calculated: ${c * k + res.count()}")

    res.takeOrdered(k)(new ResultOrdering)
  }

  def thresholdSearch(query_traj: Array[LineSegment], threshold: Double): Array[(Double, Int, Array[Byte])] = {
    val start = System.currentTimeMillis()

    val bc_query = sc.broadcast(query_traj)
    val res = DFT.candiSelection(bc_query.value, threshold, sc, compressed_traj, global_rtree, stat, traj_global_rtree, indexed_seg_rdd).persist(StorageLevel.MEMORY_AND_DISK_SER)
    //bc_query.destroy()

    println(s"==> Time to finish the final filter: ${System.currentTimeMillis() - start}ms")
    println(s"# of distance calculated: ${res.count()}")

    res.filter(item => {
      item._1 <= threshold
    }).collect()
  }

  private class ResultOrdering extends Ordering[(Double, Int, Array[Byte])] {
    override def compare(x: (Double, Int, Array[Byte]), y: (Double, Int, Array[Byte])): Int = x._1.compare(y._1)
  }

}
