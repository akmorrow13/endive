package net.akmorrow13.endive.utils

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.{TranscriptionFactors, CellTypes, PeakRecord}
import org.bdgenomics.adam.models.ReferenceRegion

class WindowLoaderSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  val tf = TranscriptionFactors.FOXA2
  val cellType = CellTypes.A549
  val region = ReferenceRegion("chr1", 0, 100)
  val sequence = "ATTTTGGGGGAAAAA"

  test("test window loader from string") {
    val peak1 = PeakRecord(region, 0, 0, 0, 0, 0)
    val window: Window = Window(tf, cellType, region, sequence, Some(List(peak1)), None)
    val labeledWindow = LabeledWindow(window, 0)
    val strWin = labeledWindow.toString
    val labeledWindow2: LabeledWindow = LabeledWindowLoader.stringToLabeledWindow(strWin)

    assert(labeledWindow2 == labeledWindow)
  }

  test("test window loader without dnase") {
    val window: Window = Window(tf, cellType,region,sequence, None, None)
    val labeledWindow = LabeledWindow(window, 0)
    val strWin = labeledWindow.toString
    val labeledWindow2: LabeledWindow = LabeledWindowLoader.stringToLabeledWindow(strWin)

    assert(labeledWindow2.label == labeledWindow.label)
  }
}
