package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite

class SamplingSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")

  sparkTest("should subsample negative points close to positive points") {
    val trainRDD = Preprocess.loadLabels(sc, labelPath)
    val sampledRDD = Sampling.selectNegativeSamples(sc, trainRDD, 700L)
    val positives = sampledRDD.filter(_._2 == 1.0)
    val negatives = sampledRDD.filter(_._2 == 0.0)
    assert(negatives.count < positives.count * 20) // number of alloted sliding windows
  }
}
