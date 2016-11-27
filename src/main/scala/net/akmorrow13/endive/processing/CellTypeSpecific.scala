package net.akmorrow13.endive.processing

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import net.akmorrow13.endive.utils.{LabeledWindow, Window}
import org.bdgenomics.adam.rdd.GenomicRegionPartitioner
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag


class CellTypeSpecific(@transient windowSize: Int,
                      @transient stride: Int,
                      dnase: RDD[(CellTypes.Value, PeakRecord)],
                      rnaseq: RDD[(CellTypes.Value, RNARecord)],
                      sd: SequenceDictionary) extends Serializable {

  def joinWithDNase(in: RDD[LabeledWindow]): RDD[LabeledWindow] = {
    val mappedDnase = CellTypeSpecific.window(dnase.map(r => (r._2.region, r._1, r._2)), sd)
    mappedDnase.setName("mappedDnasePeaks").cache()

    val str = this.stride
    val win = this.windowSize

    val x: RDD[LabeledWindow] = in.keyBy(r => (r.win.getRegion, r.win.getCellType))
      .partitionBy(GenomicRegionPartitioner(Dataset.partitions, sd))
      .leftOuterJoin(mappedDnase)
      .map(r => {
        val dnase = r._2._2.getOrElse(List())
        LabeledWindow(Window(r._2._1.win.getTf, r._2._1.win.getCellType,
          r._2._1.win.getRegion, r._2._1.win.getSequence, dnase.length), r._2._1.label)
      })
    mappedDnase.unpersist()
    x
  }

}

object CellTypeSpecific {

  /**
   * Joins datasets on ReferenceRegion and cell type
 *
   * @param rdd1
   * @param rdd2
   * @tparam T
   * @tparam S
   * @return
   */
  def joinDataSets[T: ClassTag, S: ClassTag](rdd1: RDD[(ReferenceRegion, CellTypes.Value, T)],
                                             rdd2: RDD[(ReferenceRegion, CellTypes.Value, S)]
                                              , sd: SequenceDictionary): RDD[((ReferenceRegion, CellTypes.Value), (Option[List[T]], Option[List[S]]))] = {
    val windowed1 = window(rdd1, sd)
    val windowed2 = window(rdd2, sd)
    windowed1.fullOuterJoin(windowed2)
  }

  def window[S: ClassTag, T: ClassTag](rdd: RDD[(ReferenceRegion, S, T)], sd: SequenceDictionary): RDD[((ReferenceRegion, S), List[T])] = {
    val stride = 50
    val windowSize = 200
    val windowed: RDD[((ReferenceRegion, S), List[T])]  = rdd
     .flatMap(d => {
      val newStart = d._1.start / stride * stride
      val newEnd =  d._1.end / stride * stride + stride
      val region = ReferenceRegion(d._1.referenceName, newStart, newEnd)
      unmergeRegions(region, windowSize, stride, sd).map(r => ((r, d._2), d._3))
    }).groupBy(_._1).mapValues(r => r.seq.map(_._2).toList)
    windowed.partitionBy(GenomicRegionPartitioner(Dataset.partitions, sd))
  }

  /**
   * return all sliding windows overlapping the specified region
 *
   * @param region Region to divide
   * @return list of divided smaller region
   */
  def unmergeRegions(region: ReferenceRegion, win: Int, str: Int, sd: SequenceDictionary): List[ReferenceRegion] = {
    val start = Math.max(region.start - win, 0)
    val end = Math.min(region.end + win, sd.apply(region.referenceName).get.length)
    val startValues: List[Long] = List.range(start, end, str)
    val regions = startValues.map(st => ReferenceRegion(region.referenceName, st, st + win ))
    regions.filter(r => r.overlaps(region))
  }
}
