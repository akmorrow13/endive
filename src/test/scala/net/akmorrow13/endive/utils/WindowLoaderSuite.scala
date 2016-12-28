package net.akmorrow13.endive.utils

import breeze.linalg.DenseVector
import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.{CellTypes, TranscriptionFactors}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.AlignmentRecord

class WindowLoaderSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  val tf = TranscriptionFactors.FOXA2
  val cellType = CellTypes.A549
  val region = ReferenceRegion("chr1", 0, 100)
  val sequence = "ATTTTGGGGGAAAAA"

  test("test window loader from string") {
    val peak1 =  AlignmentRecord.newBuilder()
      .setContigName(region.referenceName)
      .setStart(region.start)
      .setEnd(region.end)
      .build()

    val dnase = DenseVector.ones[Double](200) * 0.2
    val window: Window = Window(tf, cellType, region, sequence, 1, Some(dnase), None)
    val labeledWindow = LabeledWindow(window, 0)
    val strWin = labeledWindow.toString
    val labeledWindow2: LabeledWindow = LabeledWindowLoader.stringToLabeledWindow(strWin)

    assert(labeledWindow2 == labeledWindow)
  }

  test("test window loader without dnase") {
    val window: Window = Window(tf, cellType,region,sequence, 0, None, None)
    val labeledWindow = LabeledWindow(window, 0)
    val strWin = labeledWindow.toString
    val labeledWindow2: LabeledWindow = LabeledWindowLoader.stringToLabeledWindow(strWin)

    assert(labeledWindow2.label == labeledWindow.label)
  }
}
