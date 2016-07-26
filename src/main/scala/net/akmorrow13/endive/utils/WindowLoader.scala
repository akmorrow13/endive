package net.akmorrow13.endive.utils

import net.akmorrow13.endive.processing.PeakRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion

object LabeledWindowLoader {

  def stringToLabeledWindow(str: String): LabeledWindow = {
    val dataArray = str.split(",")
    val tf = dataArray(0)
    val cellType = dataArray(1)
    val region = ReferenceRegion(dataArray(2), dataArray(3).toLong,dataArray(4).toLong)
    val dnase: List[PeakRecord] = dataArray(6).split(";").map(r => PeakRecord.fromString(r)).toList
    LabeledWindow(Window(tf, cellType, region, dataArray(5), dnase),dataArray(7).toInt)
  }

  def apply(path: String, sc: SparkContext): RDD[LabeledWindow] = {
    val dataTxtRDD:RDD[String] = sc.textFile(path)
    dataTxtRDD.map(stringToLabeledWindow(_))
  }
}
