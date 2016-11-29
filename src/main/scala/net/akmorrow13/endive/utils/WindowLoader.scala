package net.akmorrow13.endive.utils

import breeze.linalg.DenseVector
import net.akmorrow13.endive.processing._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.AlignmentRecord

case class DeepbindRecord(tf: TranscriptionFactors.Value, cellType: CellTypes.Value, id: String, sequence: String, label: Int)

object DeepbindRecordLoader {

  val trainEnding = "_AC.seq.gz"
  val testEnding = "_B.seq.gz"
  val testPoints = 10000


  def load(sc: SparkContext, directory: String): (RDD[DeepbindRecord], RDD[DeepbindRecord]) = {
    // parse for tf and cellType
    val fs: FileSystem = FileSystem.get(new Configuration())
    val labelStatus = fs.listStatus(new Path(directory))
    val labels = labelStatus.head.getPath.getName.split("_")
    println(labelStatus.head.getPath.getName)
    println(labels(0), labels(1))
    val tf = TranscriptionFactors.withName(labels(0))
    val cellType = CellTypes.getEnumeration(labels(1))

    (loadTrain(sc, directory, tf, cellType),loadTest(sc, directory, tf, cellType))
  }

  private def loadTrain(sc: SparkContext,
                        directory: String,
                        tf: TranscriptionFactors.Value,
                        cellType: CellTypes.Value): RDD[DeepbindRecord] = {
    // parse for tf and cellType
    val fs: FileSystem = FileSystem.get(new Configuration())
    val labelStatus = fs.listStatus(new Path(directory))

    val trainFiles = labelStatus.filter(_.getPath.getName.contains(trainEnding))
    var trainRDD: RDD[DeepbindRecord] = sc.emptyRDD[DeepbindRecord]

    for (i <- trainFiles) {
      val file: String = i.getPath.toString
      val rdd = sc.textFile(file)
        .filter(!_.contains("Bound"))
        .map(r => stringToDeepbindRecord(r, tf, cellType))
      trainRDD = trainRDD.union(rdd)
    }
    trainRDD

  }

  private def loadTest(sc: SparkContext,
                       directory: String,
                       tf: TranscriptionFactors.Value,
                       cellType: CellTypes.Value): RDD[DeepbindRecord] = {

    val fs: FileSystem = FileSystem.get(new Configuration())
    val labelStatus = fs.listStatus(new Path(directory))

    val testFiles = labelStatus.filter(_.getPath.getName.contains(testEnding))
    var testRDD: RDD[DeepbindRecord] = sc.emptyRDD[DeepbindRecord]

    for (i <- testFiles) {
      val file: String = i.getPath.toString
      val rdd = sc.textFile(file)
        .filter(!_.contains("Bound"))
        .zipWithIndex()
        .filter(_._2 < testPoints)
        .map(r => stringToDeepbindRecord(r._1, tf, cellType))

      println(rdd.count)
      testRDD = testRDD.union(rdd)
    }
    testRDD
  }


  def stringToDeepbindRecord(str: String, tf: TranscriptionFactors.Value, cellType: CellTypes.Value): DeepbindRecord = {
    val split = str.split("\t")
    DeepbindRecord(tf, cellType, split(1), split(2), split(3).toInt)
  }
}
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
