package net.akmorrow13.endive.utils

import breeze.linalg.DenseVector
import net.akmorrow13.endive.processing._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion


object LabeledWindowLoader {

  def stringToLabeledWindow(str: String): LabeledWindow = {
    val d = str.split(Window.OUTERDELIM)
    val dataArray = d(0).split(Window.STDDELIM)

    val dnase: Option[DenseVector[Double]] = d.lift(1).map(r => {
      DenseVector(r.split(Window.STDDELIM).map(_.toDouble))
    })
    val rnaseq: Option[List[RNARecord]] = d.lift(2).map(_.split(Window.EPIDELIM).map(r => RNARecord.fromString(r)).toList)
    val motifs: Option[List[PeakRecord]] = d.lift(3).map(_.split(Window.EPIDELIM).map(r => PeakRecord.fromString(r)).toList)

    val tf = TranscriptionFactors.withName(dataArray(1))
    val cellType = CellTypes.withName(dataArray(2))
    val region = ReferenceRegion(dataArray(3), dataArray(4).toLong,dataArray(5).toLong)
    val label = dataArray(0).trim.toInt
    LabeledWindow(Window(tf, cellType, region, dataArray(6), dataArray(7).toInt, dnase = dnase, rnaseq = rnaseq, motifs = motifs), label)
  }


  def apply(path: String, sc: SparkContext): RDD[LabeledWindow] = {
    val dataTxtRDD:RDD[String] = sc.textFile(path)
    dataTxtRDD.map(stringToLabeledWindow(_))
  }
}

object FeaturizedLabeledWindowLoader {

    def stringToFeaturizedLabeledWindow(str: String): FeaturizedLabeledWindow = {
      val splitString = str.split(Window.FEATDELIM)
      val labeledWindow:LabeledWindow = LabeledWindowLoader.stringToLabeledWindow(splitString(0))
      val features:DenseVector[Double]= new DenseVector[Double](splitString(1).split(",").toArray.map(_.toDouble))
      FeaturizedLabeledWindow(labeledWindow, features)
    }

    def apply(path: String, sc: SparkContext): RDD[FeaturizedLabeledWindow] = {
      val dataTxtRDD:RDD[String] = sc.textFile(path)
      dataTxtRDD.map(stringToFeaturizedLabeledWindow(_))
    }
}
