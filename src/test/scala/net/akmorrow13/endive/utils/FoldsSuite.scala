package net.akmorrow13.endive.utils

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.{CellTypes, Chromosomes, TranscriptionFactors}
import org.bdgenomics.adam.models.ReferenceRegion

class FoldsSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")

  val tf = TranscriptionFactors.withName("ATF3")
  val NUM_CHROMOSOMES = Chromosomes.toVector.size
  val sequence = "ATTTTGGGGGAAAAA"

  sparkTest("Leave 1 out (test set is 1 cell type/chromsome pair)") {
    val NUM_CELL_TYPES = 4
    val NUM_SAMPLES = 100
    val CELL_TYPES_PER_FOLD = 1
    val CHROMOSOMES_PER_FOLD = 1

    val windowsAll  = (0 until NUM_SAMPLES).map { x =>
      ((0 until NUM_CHROMOSOMES).map { cr =>
        (0 until NUM_CELL_TYPES).map { cellType =>
        LabeledWindow(Window(tf, CellTypes.apply(cellType), ReferenceRegion("chr" + cr, 0, 100), sequence, 0, None, None), 0)
        }
      }).flatten
    }
    val windows:Seq[LabeledWindow] = windowsAll.flatten
    val windowsRDD = sc.parallelize(windows)

    /* First one chromesome and one celltype per fold (leave 1 out) */
    val folds = EndiveUtils.generateFoldsRDD(windowsRDD.keyBy(r => (r.win.region.referenceName, r.win.cellType)), CELL_TYPES_PER_FOLD, CHROMOSOMES_PER_FOLD, 1)
    val cellTypesChromosomes:Iterable[(String, CellTypes.Value)] = windowsRDD.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys

    println("TOTAL FOLDS " + folds.size)
    for (i <- (0 until folds.size)) {
      println("FOLD " + i)
      val train = folds(i)._1.map(_._2)
      val test = folds(i)._2.map(_._2)

      println("TRAIN SIZE IS " + train.count())
      println("TEST SIZE IS " + test.count())

      val cellTypesTest:Iterable[CellTypes.Value] = test.map(x => (x.win.cellType)).countByValue().keys
      val chromosomesTest:Iterable[String] = test.map(x => (x.win.getRegion.referenceName)).countByValue().keys
      val cellTypesChromosomesTest:Iterable[(String, CellTypes.Value)] = test.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys
      println(cellTypesTest.size)
      println(chromosomesTest.size)

      assert(cellTypesTest.size == CELL_TYPES_PER_FOLD)
      assert(chromosomesTest.size == CHROMOSOMES_PER_FOLD)
      val cellTypesTrain:Iterable[CellTypes.Value] = train.map(x => (x.win.cellType)).countByValue().keys
      val chromosomesTrain:Iterable[String] = train.map(x => (x.win.getRegion.referenceName)).countByValue().keys

      assert(cellTypesTrain.size == NUM_CELL_TYPES - CELL_TYPES_PER_FOLD)
      assert(chromosomesTrain.size == NUM_CHROMOSOMES - CHROMOSOMES_PER_FOLD)
    }
  }

  sparkTest("Leave 3 out (test set is 3 cell type/chromsome pair)") {
    val NUM_CELL_TYPES = 10
    val NUM_SAMPLES = 100
    val CELL_TYPES_PER_FOLD = 3
    val CHROMOSOMES_PER_FOLD = 3

    val windowsAll  = (0 until NUM_SAMPLES).map { x =>
      ((0 until NUM_CHROMOSOMES).map { cr =>
        (0 until NUM_CELL_TYPES).map { cellType =>
        LabeledWindow(Window(tf, CellTypes.apply(cellType), ReferenceRegion("chr" + cr, 0, 100), sequence, 0, None, None), 0)
        }
      }).flatten
    }
    val windows:Seq[LabeledWindow] = windowsAll.flatten
    val windowsRDD = sc.parallelize(windows)

    /* First one chromesome and one celltype per fold (leave 1 out) */
    val folds = EndiveUtils.generateFoldsRDD(windowsRDD.keyBy(r => (r.win.region.referenceName, r.win.cellType)), CELL_TYPES_PER_FOLD, CHROMOSOMES_PER_FOLD, 1)
    val cellTypesChromosomes:Iterable[(String, CellTypes.Value)] = windowsRDD.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys

    println("TOTAL FOLDS " + folds.size)
    for (i <- (0 until folds.size)) {
      println("FOLD " + i)
      val train = folds(i)._1.map(_._2)
      val test = folds(i)._2.map(_._2)

      println("TRAIN SIZE IS " + train.count())
      println("TEST SIZE IS " + test.count())

      val cellTypesTest = test.map(x => (x.win.cellType)).countByValue().keys
      val chromosomesTest:Iterable[String] = test.map(x => (x.win.getRegion.referenceName)).countByValue().keys
      val cellTypesChromosomesTest = test.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys
      println(cellTypesTest.size)
      println(chromosomesTest.size)

      assert(cellTypesTest.size == CELL_TYPES_PER_FOLD)
      assert(chromosomesTest.size == CHROMOSOMES_PER_FOLD)
      val cellTypesTrain = train.map(x => (x.win.cellType)).countByValue().keys
      val chromosomesTrain = train.map(x => (x.win.getRegion.referenceName)).countByValue().keys

      assert(cellTypesTrain.size == NUM_CELL_TYPES - CELL_TYPES_PER_FOLD)
      assert(chromosomesTrain.size == NUM_CHROMOSOMES - CHROMOSOMES_PER_FOLD)
    }
  }

}
