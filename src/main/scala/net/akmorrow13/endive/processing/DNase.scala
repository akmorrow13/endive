package net.akmorrow13.endive.processing

import net.akmorrow13.endive.processing.PeakRecord
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import net.akmorrow13.endive.utils.{LabeledWindow, Window}

import scala.collection.mutable.ListBuffer


class DNase(@transient windowSize: Int,
                      @transient stride: Int,
                      dnase: RDD[(String, PeakRecord)]) extends Serializable {

  /**
   * merges sequences with overlapping dnase regions
   * @param in Window of sequences specified by cell type and transcription factor
   * @return new window with dnase regions
   */
   def joinWithSequences(in: RDD[LabeledWindow]): RDD[LabeledWindow] = {
      // map dnase to window sizes that match the input window sizes

    val str = this.stride
    val win = this.windowSize
    val windowedDnase: RDD[((ReferenceRegion, String), List[PeakRecord])]  = dnase.flatMap(d => {
      val newStart = d._2.region.start / str * str
      val newEnd =  d._2.region.end / str * str + str
      val region = ReferenceRegion(d._2.region.referenceName, newStart, newEnd)
      unmergeRegions(region, win, str).map(r => ((r,d._1), d._2))
    }).groupBy(_._1).mapValues(r => r.seq.map(_._2).toList)

      val x: RDD[LabeledWindow] = in.keyBy(r => (r.win.region, r.win.cellType))
        .leftOuterJoin(windowedDnase)
        .map(r => LabeledWindow(Window(r._2._1.win.tf, r._2._1.win.cellType,
        r._2._1.win.region, r._2._1.win.sequence,r._2._2.getOrElse(List())), r._2._1.label))
      x

    }

  /**
   * take region and divide it up into chunkSize regions
   * @param region Region to divide
   * @return list of divided smaller region
   */
  def unmergeRegions(region: ReferenceRegion, win: Int, str: Int): List[ReferenceRegion] = {
    val startValues: List[Long] = List.range(region.start, region.end + win, str)
    startValues.map(st => ReferenceRegion(region.referenceName, st, st + win ))
  }
}