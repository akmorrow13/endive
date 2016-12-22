package net.akmorrow13.endive.processing

import breeze.linalg.DenseVector
import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.bdgenomics.adam.models.ReferenceRegion

class PreprocessSuite extends EndiveFunSuite {

  test("should save and read baserecord") {

    val win = Window(TranscriptionFactors.ARID3A, CellTypes.A549, ReferenceRegion("chr1", 600, 800), "AAAA", 1)
    val lwindow = LabeledWindow(win, 1)
    val features = DenseVector[Double](0.0,0.1,0.2,1.6)

    val baseFeat = BaseFeature(lwindow, features)
    val baseStr = baseFeat.toString
    val recovered = BaseFeatureLoader.stringToBaseFeature(baseStr)

    assert(recovered.features.length == features.length)
    assert(recovered.labeledWindow.win.tf == TranscriptionFactors.ARID3A)
  }
}

