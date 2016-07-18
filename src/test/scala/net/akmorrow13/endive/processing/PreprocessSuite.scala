package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite

class PreprocessSuite extends EndiveFunSuite {

  // training data of region and labels
  var peakPath = resourcePath("DNASE.A549.conservative.head30.narrowPeak")

  sparkTest("should extract peak records from narrowPeak file") {
    val trainRDD = Preprocess.loadPeaks(sc, peakPath)
    val rec = trainRDD.first
    assert(trainRDD.count == 30)
    assert(rec._2.score == 866)
    assert(rec._2.signalValue == 1.31833)
    assert(rec._2.pValue == 2.19291)
    assert(rec._2.qValue == 0.61886)
    assert(rec._2.peak == 724)
  }
}
