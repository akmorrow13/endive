package net.akmorrow13.endive.processing

import net.akmorrow13.endive.processing.PeakRecord
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import net.akmorrow13.endive.utils.{LabeledWindow, Window, LabeledReferenceRegionPartitioner}
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag


class CellTypeSpecific(@transient windowSize: Int,
                      @transient stride: Int,
                      dnase: RDD[(String, PeakRecord)],
                      rnaseq: RDD[(String, RNARecord)],
                      sd: SequenceDictionary) extends Serializable {

  def joinWithDNase(in: RDD[LabeledWindow]): RDD[LabeledWindow] = {
    val mappedDnase = CellTypeSpecific.window(dnase.map(r => (r._2.region, r._1, r._2)), sd)

    val str = this.stride
    val win = this.windowSize

    println("cell type specific partition count", in.partitions.length)
    // TODO: this does not calculate held out chrs
    val x: RDD[LabeledWindow] = in.keyBy(r => (r.win.getRegion, r.win.getCellType))
     // .partitionBy(new LabeledReferenceRegionPartitioner(sd, Dataset.cellTypes.toVector))
      .leftOuterJoin(mappedDnase)
      .map(r => {
        val dnase = r._2._2.getOrElse(List())
        LabeledWindow(Window(r._2._1.win.getTf, r._2._1.win.getCellType,
          r._2._1.win.getRegion, r._2._1.win.getSequence, dnase = Some(dnase)), r._2._1.label)
      })
    x.repartition(20)
  }

  /**
   * merges sequences with overlapping dnase regions
   * @param in Window of sequences specified by cell type and transcription factor
   * @return new window with dnase regions
   */
   def joinWithSequences(in: RDD[LabeledWindow]): RDD[LabeledWindow] = {
    // if no rnaseq just join with dnase
    if (rnaseq.isEmpty)
      return joinWithDNase(in)

    // map dnase and rnaseq to window sizes that match the input window sizes
    val mappedDnase = dnase.map(r => (r._2.region, r._1, r._2))
    val mappedRnaseq = rnaseq.map(r => (r._2.region, r._1, r._2))

    // join together cell type specific information
    val cellData: RDD[((ReferenceRegion, String), (Option[List[PeakRecord]], Option[List[RNARecord]]))]  =
      CellTypeSpecific.joinDataSets(mappedDnase, mappedRnaseq, sd)

    val str = this.stride
    val win = this.windowSize


    // TODO: this does not calculate held out chrs
      val x: RDD[LabeledWindow] = in.keyBy(r => (r.win.getRegion, r.win.getCellType))
        //  .partitionBy(new LabeledReferenceRegionPartitioner(sd, Dataset.cellTypes.toVector))
        .leftOuterJoin(cellData)
        .map(r => {
          val (dnase, rnaseq) =
            if (r._2._2.isDefined) {
              (r._2._2.get._1.getOrElse(List()),  r._2._2.get._2.getOrElse(List()))
            } else (List(), List())
          LabeledWindow(Window(r._2._1.win.getTf, r._2._1.win.getCellType,
            r._2._1.win.getRegion, r._2._1.win.getSequence,dnase = Some(dnase),rnaseq = Some(rnaseq)), r._2._1.label)
        })
      x
    }

}

object CellTypeSpecific {

  /**
   * Joins datasets on ReferenceRegion and cell type
   * @param rdd1
   * @param rdd2
   * @tparam T
   * @tparam S
   * @return
   */
  def joinDataSets[T: ClassTag, S: ClassTag](rdd1: RDD[(ReferenceRegion, String, T)],
                                             rdd2: RDD[(ReferenceRegion, String, S)]
                                              , sd: SequenceDictionary): RDD[((ReferenceRegion, String), (Option[List[T]], Option[List[S]]))] = {
    val windowed1 = window[T](rdd1, sd)
    val windowed2 = window[S](rdd2, sd)
    windowed1.fullOuterJoin(windowed2)
  }

  def window[T: ClassTag](rdd: RDD[(ReferenceRegion, String, T)], sd: SequenceDictionary): RDD[((ReferenceRegion, String), List[T])] = {
    val stride = 50
    val windowSize = 200
    val windowed: RDD[((ReferenceRegion, String), List[T])]  = rdd.flatMap(d => {
      val newStart = d._1.start / stride * stride
      val newEnd =  d._1.end / stride * stride + stride
      val region = ReferenceRegion(d._1.referenceName, newStart, newEnd)
      unmergeRegions(region, windowSize, stride).map(r => ((r, d._2), d._3))
    }).groupBy(_._1).mapValues(r => r.seq.map(_._2).toList)
    //val x=windowed.partitionBy(new LabeledReferenceRegionPartitioner(sd, Dataset.cellTypes.toVector))
    windowed.repartition(20)
   
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
