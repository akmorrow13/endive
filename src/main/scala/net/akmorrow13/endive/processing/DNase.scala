package net.akmorrow13.endive.processing

import net.akmorrow13.endive.processing.PeakRecord
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import net.akmorrow13.endive.utils.{LabeledWindow, Window}

import scala.collection.mutable.ListBuffer


class DNase(windowSize: Int,
                      stride: Int,
                      dnase: RDD[(String, PeakRecord)]) {

  /**
   * merges sequences with overlapping dnase regions
   * @param in Window of sequences specified by cell type and transcription factor
   * @return new window with dnase regions
   */
   def joinWithSequences(in: RDD[LabeledWindow]): RDD[LabeledWindow] = {
      // map dnase to window sizes that match the input window sizes
      val windowedDnase: RDD[((ReferenceRegion, String), List[PeakRecord])] = dnase.flatMap(d => {
        val start = d._2.region.start
        val end = d._2.region.end
        val name = d._2.region.referenceName
        val newStart = start / stride * stride
        val newEnd = end / stride *stride + stride
        val newRegion = ReferenceRegion(name, newStart, newEnd)
        unmergeRegions(newRegion).map(r => ((r,d._1), d._2))
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
  def unmergeRegions(region: ReferenceRegion): List[ReferenceRegion] = {
    var regions: ListBuffer[ReferenceRegion] = new ListBuffer[ReferenceRegion]()
    var start = region.start
    var end = start + windowSize

    while (start < region.end + windowSize ) {
      val r = new ReferenceRegion(region.referenceName, start, end)
      regions += r
      start += stride
      end += stride
    }

    regions.toList
  }
}