package net.akmorrow13.endive.utils

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.PeakRecord
import org.bdgenomics.adam.models.ReferenceRegion

class FoldsSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")


  sparkTest("Leave 1 out (test set is 1 cell type/chromsome pair)") {
    val NUM_CELL_TYPES = 4
    val NUM_CHROMOSOMES = 24
    val NUM_SAMPLES = 10000
    val CELL_TYPES_PER_FOLD = 1
    val CHROMOSOMES_PER_FOLD = 1
    val cellType0 = "cellType0"
    val cellType1 = "cellType1"
    val region0 = ReferenceRegion("chr0", 0, 100)
    val region1 = ReferenceRegion("chr1", 0, 100)
    val sequence = "ATTTTGGGGGAAAAA"
    val tf = "ATF3"


    val windows: Seq[LabeledWindow] = (0 until NUM_SAMPLES).map{
      (0 until NUM_CHROMOSOMES).map { cr =>
        (0 until NUM_CELL_TYPES).map { cellType =>
        LabeledWindow(Window(tf, cellType.toString, ReferenceRegion("chr" + cr, 0, 100), sequence, None, None), 0)
        }
      }
    }.flatten
    val windowsRDD = sc.parallelize(windows)

    /* First one chromesome and one celltype per fold (leave 1 out) */
    val folds = EndiveUtils.generateFoldsRDD(windowsRDD, 1, 1, 1)

    for (i <- (0 until folds.size)) {
      val train = folds(i)._1
      val test = folds(i)._2

      val cellTypesTest:Iterable[String] = test.map(x => (x.win.cellType)).countByValue().keys
      val chromosomesTest:Iterable[String] = test.map(x => (x.win.getRegion.referenceName)).countByValue().keys
      assert(cellTypesTest.size == CELL_TYPES_PER_FOLD)
      assert(chromosomesTest.size == CELL_TYPES_PER_FOLD)

      val cellTypesTrain:Iterable[String] = train.map(x => (x.win.cellType)).countByValue().keys
      val chromosomesTrain:Iterable[String] = train.map(x => (x.win.getRegion.referenceName)).countByValue().keys
      assert(NUM_CELL_TYPES  == cellTypesTest.size + cellTypesTrain.size)
      assert(NUM_CELL_TYPES*NUM_CHROMOSOMES  == cellTypesTest.size*chromosomesTest.size + cellTypesTrain.size*chromosomesTrain.size)
    }
  }

}
