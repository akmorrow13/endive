package net.akmorrow13.endive.utils

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.PeakRecord
import org.bdgenomics.adam.models.ReferenceRegion

class WindowLoaderSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")

  test("test window loader from string") {
    val tf = "FOXA2"
    val cellType = "cellType"
    val region = ReferenceRegion("chr1", 0, 100)
    val sequence = "ATTTTGGGGGAAAAA"
    val peak1 = PeakRecord(region, 0, 0, 0, 0, 0)

    val window: Window = Window(tf, cellType,region,sequence, List(peak1), List())
    val labeledWindow = LabeledWindow(window, 0)
    val strWin = labeledWindow.toString
    val labeledWindow2: LabeledWindow = LabeledWindowLoader.stringToLabeledWindow(strWin)
    assert(labeledWindow2 == labeledWindow)
  }
}
