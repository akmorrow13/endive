package net.akmorrow13.endive.utils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LabeledWindowLoader {

  def stringToLabeledWindow(str: String): LabeledWindow = {
    val dataArray = str.split(",")
    LabeledWindow(Window(dataArray(0).toInt,dataArray(1).toInt,dataArray(2)),dataArray(4).toInt)
  }

  def apply(path: String, sc: SparkContext): RDD[LabeledWindow] = {
    val dataTxtRDD:RDD[String] = sc.textFile(path)
    dataTxtRDD.map(stringToLabeledWindow(_))
  }
}
