package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary}

class SamplingSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")

  sparkTest("should subsample negative points close to positive points") {
    val trainRDD: RDD[LabeledWindow] = Preprocess.loadLabels(sc, labelPath)
                                .map(r => {
                                  val win = Window(r._1, r._2, r._3, "AAAAA", None, None)
                                  LabeledWindow(win, r._4)
                                })
    val sd = new SequenceDictionary(Dataset.chrs.map(r => SequenceRecord(r, 10000000)).toVector)

    val sampledRDD = Sampling.subselectSamples(sc, trainRDD, 700L, sd)
    val positives = sampledRDD.filter(_.label == 1.0)
    val negatives = sampledRDD.filter(_.label == 0.0)
    assert(negatives.count < positives.count * 20) // number of alloted sliding windows
  }
}
