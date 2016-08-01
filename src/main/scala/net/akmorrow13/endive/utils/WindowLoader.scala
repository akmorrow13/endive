package net.akmorrow13.endive.utils

import java.io.ByteArrayInputStream
import net.akmorrow13.endive.processing.{RNARecord, PeakRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import scala.util.{ Try, Success, Failure }

object LabeledWindowLoader {

  def stringToLabeledWindow(str: String): LabeledWindow = {
    val d = str.split(Window.OUTERDELIM)
    val dataArray = d(0).split(Window.CHIPSEQDELIM)
    if (d.size > 1) {
      System.err.println("str IS " + str)
      System.err.println("d(0) " + d(0))
      System.err.println("d(1) " + d(1))
    }
    val dnase: Option[List[PeakRecord]] = d.lift(1).map(_.split(Window.EPIDELIM).map(r => PeakRecord.fromString(r)).toList)
    val rnaseq: Option[List[RNARecord]] = d.lift(2).map(_.split(Window.EPIDELIM).map(r => RNARecord.fromString(r)).toList)
    val tf = dataArray(1)
    val cellType = dataArray(2)
    val region = ReferenceRegion(dataArray(3), dataArray(4).toLong,dataArray(5).toLong)
    val label = dataArray(0).trim.toInt
    LabeledWindow(Window(tf, cellType, region, dataArray(6), dnase = dnase, rnaseq = rnaseq), label)
  }

  /*
  def byteArrayToLabeledWindow(ba: Array[Byte]): LabeledWindow = {
    val intSeqType = AvroType[LabeledWindow]
    val io: AvroTypeIO[LabeledWindow] = intSeqType.io
    val is = new ByteArrayInputStream(ba);
    val Success(readResult) = io read is
    readResult
  }
  */

  def apply(path: String, sc: SparkContext): RDD[LabeledWindow] = {
    val dataTxtRDD:RDD[String] = sc.textFile(path)
    dataTxtRDD.map(stringToLabeledWindow(_))
  }
}
