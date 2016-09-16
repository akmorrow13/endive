package net.akmorrow13.endive.utils

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing._
import org.bdgenomics.adam.models.{ReferencePosition, ReferenceRegion}

class CutLoaderSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  val tf = TranscriptionFactors.FOXA2
  val cellType = CellTypes.A549
  val region = ReferenceRegion("chr1", 0, 100)
  val sequence = "ATTTTGGGGGAAAAA"
  val cut1: CutMap = CutMap(ReferencePosition("chr1", 5), Map((CellTypes.A549 -> 1), (CellTypes.GM12878 -> 2)))
  val cut2: CutMap = CutMap(ReferencePosition("chr1", 8), Map((CellTypes.A549 -> 1), (CellTypes.GM12878 -> 2)))

  test("loads cut from string") {
    val str = cut1.toString
    val newCut = CutMapLoader.fromString(str)
    assert(cut1 == newCut)
  }


  test("test window loader from string") {
    val peak1 = PeakRecord(region, 0, 0, 0, 0, 0)
    val window: Window = Window(tf, cellType, region, sequence, Some(List(peak1)), None)
    val labeledWindow = LabeledWindow(window, 0)
    val strWin = labeledWindow.toString
    val cuts: Iterable[CutMap] = Iterable(cut1, cut2)
    val str = (strWin + "/" + cuts.map(_.toString).mkString(":"))

    val(newWin, newCuts) = LabeledCutMapLoader.stringToLabeledCuts(str)
    assert(labeledWindow == newWin)
    assert(cuts == newCuts)
  }

  test("test window loader without cuts") {
    val peak1 = PeakRecord(region, 0, 0, 0, 0, 0)
    val window: Window = Window(tf, cellType, region, sequence, Some(List(peak1)), None)
    val labeledWindow = LabeledWindow(window, 0)
    val strWin = labeledWindow.toString
    val str = (strWin + "/")

    val(newWin, newCuts) = LabeledCutMapLoader.stringToLabeledCuts(str)
    assert(labeledWindow == newWin)
    assert(newCuts.size == 0)
  }
}
