package net.akmorrow13.endive.processing

import net.akmorrow13.endive.processing.PeakRecord
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import net.akmorrow13.endive.utils.{LabeledWindow, Window, LabeledReferenceRegionPartitioner}
import scala.collection.mutable.ListBuffer
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.features.CoverageRDD
import scala.reflect.ClassTag


class Dnase(@transient windowSize: Int,
                       @transient stride: Int,
                        @transient sc: SparkContext,
                       dnaseCoverage: CoverageRDD) extends Serializable {

//  def joinWithDNase(in: RDD[LabeledWindow]): RDD[LabeledWindow] = {

//    val flattened = dnaseCoverage.flatten
//
//    val str = this.stride
//    val win = this.windowSize
//
//    println("cell type specific partition count", in.partitions.length)
//    val x: RDD[LabeledWindow] = in.keyBy(r => (r.win.getRegion, r.win.getCellType))
//      .partitionBy(new LabeledReferenceRegionPartitioner(sd, Dataset.cellTypes.toVector))
//      .leftOuterJoin(mappedDnase)
//      .map(r => {
//        val dnase = r._2._2.getOrElse(List())
//        LabeledWindow(Window(r._2._1.win.getTf, r._2._1.win.getCellType,
//          r._2._1.win.getRegion, r._2._1.win.getSequence, dnase = Some(dnase)), r._2._1.label)
//      })
//    x
//  }
//
//  def coverage2Window(bamFile: String, sd: SequenceDictionary) {
//    val alignments = sc.loadAlignments(bamFile)
//
//    val coverage: CoverageRDD = alignments.toCoverage(false)
//
//    val wholeGenome: RDD[(String, Long)] =
//      sc.parallelize(sd.records.flatMap(r => (0 until r.length).map(i => (r.name, i))).toSeq)
//
//    coverage.rdd.keyBy(r => (r.contigName, r.start))
//
//  }
}

case class Cut(region: ReferenceRegion, experimentId: String, readId: String, negativeStrand: Boolean) {
  override
  def toString: String = {
    s"${region.referenceName},${region.start},${region.end},${experimentId},${readId},${negativeStrand}"
  }
}

case class DnaseWindow(region: ReferenceRegion, counts: Array[Int])