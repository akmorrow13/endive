package net.akmorrow13.endive.utils

import net.akmorrow13.endive.processing.PeakRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion

object LabeledWindowLoader {

  def stringToLabeledWindow(str: String): LabeledWindow = {
    val d = str.split(";")
    val dataArray = d(0).split(",")
    val dnase: List[PeakRecord] = d.drop(1).map(r => PeakRecord.fromString(r)).toList
    val tf = dataArray(1)
    val cellType = dataArray(2)
    val region = ReferenceRegion(dataArray(3), dataArray(4).toLong,dataArray(5).toLong)
    LabeledWindow(Window(tf, cellType, region, dataArray(6), dnase),dataArray(0).toInt)
}

  def apply(path: String, sc: SparkContext): RDD[LabeledWindow] = {
    val dataTxtRDD:RDD[String] = sc.textFile(path)
    dataTxtRDD.map(stringToLabeledWindow(_))
  }
}
