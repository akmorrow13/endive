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
    val window: Window = Window(tf, cellType, region, sequence, 0, None, None)
    val labeledWindow = LabeledWindow(window, 0)
    val strWin = labeledWindow.toString
    val labeledWindow2: LabeledWindow = LabeledWindowLoader.stringToLabeledWindow(strWin)

    assert(labeledWindow2.label == labeledWindow.label)
  }

  test("assert 2 windows correctly merge") {

    val region1 = ReferenceRegion("chr1", 200, 400)
    val region2 = ReferenceRegion("chr1", 350, 550)

    val sequence1 = "A" * 200
    val sequence2 = "G" * 200

    val dnase1 = DenseVector(Array.fill[Double](200)(0.1))
    val dnase2 = DenseVector(Array.fill[Double](200)(0.2))

    val window1 = Window(tf, cellType, region1, sequence1, 2, Some(dnase1))
    val window2 = Window(tf, cellType, region2, sequence2, 2, Some(dnase2))

    val mergedWindow = window1.merge(window2)
    assert(mergedWindow.getRegion.start == 200 && mergedWindow.getRegion.end == 550)
    assert(mergedWindow.getSequence.length == mergedWindow.getRegion.length)
    assert(mergedWindow.getDnase.length == mergedWindow.getRegion.length)

    println(mergedWindow.getSequence.count(_ =='A'))

    println(mergedWindow.getSequence.count(_ =='G'))
    assert(mergedWindow.getSequence == ("A"*200).concat("G"*150))
    assert(mergedWindow.getDnase == DenseVector.vertcat(DenseVector(Array.fill(200)(0.1)), DenseVector(Array.fill(150)(0.2))))

  }

  test("slices window") {

    val region = ReferenceRegion("chr1", 100, 300)
    val sequence = "A" * 50 + "GG"*100 + "T" * 50
    val dnase = DenseVector(Array.fill(50)(1.0) ++ Array.fill(100)(0.1) ++ Array.fill(50)(1.0))
    val window: Window = Window(tf, cellType, region, sequence, 0, Some(dnase), None)

    val center = region.length()/2 + region.start

    val newLength = 100
    val half = newLength/2
    val sliced = window.slice(center - half, center + half)

    assert(sliced.getRegion.length == newLength)
    assert(sliced.getSequence.length == newLength)
    assert(sliced.getDnase.length == newLength)

    assert(!sliced.getSequence.contains('A') && !sliced.getSequence.contains('T'))
    assert(sliced.getDnase.valueAt(0) == 0.1)
    assert(sliced.getDnase.valueAt(newLength-1) == 0.1)

  }
}
