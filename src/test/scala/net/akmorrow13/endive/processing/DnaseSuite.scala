package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.pipelines.DatasetCreationPipeline
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion

class DNaseSuite extends EndiveFunSuite {

  // training data of region and labels
  var peakPath = resourcePath("DNASE.A549.conservative.head30.narrowPeak")
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  var windowSize = 200
  val stride = 50

  sparkTest("should merge dnase and labels") {
    val dnaseRDD = Preprocess.loadPeaks(sc, peakPath)
    val labels: RDD[(String, String, ReferenceRegion, Int)] = Preprocess.loadLabelFolder(sc, labelPath)
    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] =
      labels.map(r => LabeledWindow(Window(r._1, r._2, r._3, "ATGCG" * 40, List()), r._4))
    val dnase = new DNase(windowSize, stride, dnaseRDD)
    val merged = dnase.joinWithSequences(sequences)
    println(merged)

  }
}
