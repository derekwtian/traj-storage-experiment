package edu.rmit.cidda.dft

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import edu.utah.cs.index.RTree
import edu.utah.cs.index_rr.RTreeWithRR
import edu.utah.cs.partitioner.{STRSegPartition, STRTrajPartition}
import edu.utah.cs.spatial.{LineSegment, MBR, Point}
import edu.utah.cs.trajectory.{TrajMeta, Trajectory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable

object DFT {
  final val max_entries_per_node = 25

  def getMBR(x: Array[LineSegment]): MBR = {
    val pts = x.flatMap(p => Array(p.start, p.end))
    var maxx = Double.MinValue
    var maxy = Double.MinValue
    var minx = Double.MaxValue
    var miny = Double.MaxValue
    pts.foreach(x => {
      maxx = Math.max(x.coord(0), maxx)
      maxy = Math.max(x.coord(1), maxy)
      minx = Math.min(x.coord(0), minx)
      miny = Math.min(x.coord(1), miny)
    })
    MBR(Point(Array(minx, miny)), Point(Array(maxx, maxy)))
  }

  private class ResultOrdering extends Ordering[(Double, Int)] {
    override def compare(x: (Double, Int), y: (Double, Int)): Int = x._1.compare(y._1)
  }

  def buildIndex(dataRDD: RDD[(LineSegment, TrajMeta)], trajs: RDD[(MBR, (Int, Array[LineSegment]))]) = {
    val part_traj = STRTrajPartition(trajs, dataRDD.partitions.length, 0.01, max_entries_per_node)

    val compressed_traj = part_traj.mapPartitions(iter => iter.map(x => {
      val baos = new ByteArrayOutputStream()
      val gzipOut = new GZIPOutputStream(baos)
      val objectOut = new ObjectOutputStream(gzipOut)
      objectOut.writeObject(x._2._2)
      objectOut.close()
      (x._2._1, baos.toByteArray)
    })).persist(StorageLevel.MEMORY_AND_DISK_SER)

    println(compressed_traj.count)

    val traj_stat = part_traj.mapPartitions(iter => {
      Array(iter.aggregate[(MBR, Int)]((null, 0))((res, now) => {
        if (res._1 == null) (now._1, 1)
        else (res._1.union(now._1), res._2 + 1)
      }, (left, right) => {
        if (left._1 == null) right
        else if (left._1 == null) left
        else (left._1.union(right._1), left._2 + right._2)
      })).iterator
    }).collect()
    val traj_global_rtree =
      RTree.applyMBR(traj_stat.zipWithIndex.map(x => (x._1._1, x._2, x._1._2)), max_entries_per_node)


    val (partitioned_rdd, _) = STRSegPartition(dataRDD, dataRDD.partitions.length, 0.01, max_entries_per_node)

    val indexed_seg_rdd = partitioned_rdd.mapPartitions(iter => {
      val data = iter.toArray
      var index: RTreeWithRR = if (data.length > 0) {
        RTreeWithRR(data.zipWithIndex.map(x => (x._1._1, x._2, x._1._2.traj_id)), max_entries_per_node)
      } else null
      Array(index).iterator
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    val stat = indexed_seg_rdd.mapPartitions(iter => iter.map(x => (x.root.m_mbr, x.root.size, x.root.rr))).collect()

    val global_rtree = RTree.applyMBR(stat.zipWithIndex.map(x => (x._1._1, x._2, x._1._2.toInt)), max_entries_per_node)

    (compressed_traj, traj_global_rtree, indexed_seg_rdd, stat, global_rtree)
  }

  def calcPruningBound(query_traj: Array[LineSegment], k: Int, c: Int, sc: SparkContext, compressed_traj: RDD[(Int, Array[Byte])], global_rtree: RTree, stat: Array[(MBR, Long, RoaringBitmap)], traj_global_rtree: RTree): Double = {
    val global_intersect = global_rtree.circleRange(query_traj, 0.0)
    val global_intersect_mbrs = global_intersect.map(_._1.asInstanceOf[MBR])

    val sample_base = global_intersect.aggregate(new RoaringBitmap())((a, b) => RoaringBitmap.or(a, stat(b._2)._3),
      (a, b) => RoaringBitmap.or(a, b))

    val cards = sample_base.getCardinality
    println("Cardinality of intersected Partitions: " + cards)
    val n_samples = c * k
    println("Going to sample: " + n_samples)
    assert(cards >= k)

    val set = mutable.HashSet[Int]()
    val rnd = scala.util.Random

    for (i <- 0 until n_samples) {
      var x = rnd.nextInt(cards)
      while (set.contains(x)) x = rnd.nextInt(cards)
      set += x
    }

    var i = 0
    val samples = mutable.HashSet[Int]()
    val iter = sample_base.iterator()
    while (iter.hasNext) {
      val x = iter.next()
      if (set.contains(i)) samples += x
      i = i + 1
    }

    val bc_samples = sc.broadcast(samples.toSet)
    val pruning_bound_filter = traj_global_rtree.circleRange(global_intersect_mbrs, 0.0).map(_._2).toSet
    val pruning_bound = new PartitionPruningRDD(compressed_traj, pruning_bound_filter.contains)
      .filter(x => bc_samples.value.contains(x._1))
      .repartition(Math.min(samples.size, sc.defaultParallelism))
      .map(x => {
        //Trajectory.hausdorffDistance(query_traj, content)
        Trajectory.discreteFrechetDistance(query_traj, trajReconstruct(x._2))
      })
      .takeOrdered(k).last
    //bc_samples.destroy()

    pruning_bound
  }

  def candiSelection(query_traj: Array[LineSegment], pruning_bound: Double, sc: SparkContext, compressed_traj: RDD[(Int, Array[Byte])], global_rtree: RTree, stat: Array[(MBR, Long, RoaringBitmap)], traj_global_rtree: RTree, indexed_seg_rdd: RDD[RTreeWithRR]): RDD[(Double, Int, Array[Byte])] = {
    //calculate all saved traj_ids
    val global_prune = global_rtree.circleRange(query_traj, pruning_bound)
    val global_prune_set = global_prune.map(_._2).toSet

    val pruned_rdd = new PartitionPruningRDD(indexed_seg_rdd, global_prune_set.contains)
    val pruned_traj_id1 = stat.zipWithIndex.filter(x => !global_prune_set.contains(x._2)).map(_._1._3)
      .aggregate(new RoaringBitmap())((a, b) => RoaringBitmap.or(a, b), (a, b) => RoaringBitmap.or(a, b))

    val bc_pruning_bound = sc.broadcast(pruning_bound)
    val pruned_traj_id2 = pruned_rdd.map(part => {
      part.antiCircleRangeBF(query_traj, bc_pruning_bound.value)
    }).reduce((a, b) => RoaringBitmap.or(a, b))

    val tot_pruned_traj = RoaringBitmap.or(pruned_traj_id1, pruned_traj_id2)
    //val tot_prune_count = tot_pruned_traj.getCardinality

    //bc_pruning_bound.destroy()


    //final filter
    val bc_pruned_traj = sc.broadcast(tot_pruned_traj)

    val final_prune_set = traj_global_rtree.circleRange(global_prune.map(_._1.asInstanceOf[MBR]), 0.0).map(_._2).toSet
    val final_filtered = new PartitionPruningRDD(compressed_traj, final_prune_set.contains)
      .filter(x => !bc_pruned_traj.value.contains(x._1))

    //bc_pruned_traj.destroy()

    final_filtered.repartition(sc.defaultParallelism)
      .mapPartitions(iter => iter.map(x =>{
        //(Trajectory.hausdorffDistance(query_traj, content), x._1)
        (Trajectory.discreteFrechetDistance(query_traj, trajReconstruct(x._2)), x._1, x._2)
      }))
  }

  def trajReconstruct(trajCompressed: Array[Byte]): Array[LineSegment] = {
    val bais = new ByteArrayInputStream(trajCompressed)
    val gzipIn = new GZIPInputStream(bais)
    val objectIn = new ObjectInputStream(gzipIn)
    val traj = objectIn.readObject().asInstanceOf[Array[LineSegment]]
    traj
  }

}
