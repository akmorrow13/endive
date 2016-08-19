package net.akmorrow13.endive.utils

import net.akmorrow13.endive.processing.CellTypes
import org.apache.spark.rdd.RDD
import scala.util.Random

object EndiveUtils {

/* Generate folds RDD */
def generateFoldsRDD(allData:RDD[LabeledWindow], numHeldOutCellTypes: Int = 1, numHeldOutChromosomes: Int = 3, numFolds: Int = 10, sampleFreq: Double = 0.001, randomSeed:Int = 0) = {

    @transient
    val r = new Random(randomSeed)

    val sampledData = allData.sample(false, sampleFreq)

    val cellTypesChromosomes:Set[(String, CellTypes.Value)] = sampledData.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys.toSet

    /* this will work with exponentially high probability */
    val cellTypes:Iterable[CellTypes.Value] = sampledData.map(x => (x.win.cellType)).countByValue().keys

    /* this will work with exponentially high probability */
    val chromosomes:Iterable[String] = sampledData.map(x => (x.win.getRegion.referenceName)).countByValue().keys

    for (i <- (0 until numFolds)) yield
        {
        val holdOutCellTypes = r.shuffle(cellTypes).take(numHeldOutCellTypes).toSet
        val holdOutChromosomes = r.shuffle(chromosomes).take(numHeldOutChromosomes).toSet
        generateTrainTestSplit(allData, holdOutCellTypes, holdOutChromosomes)
        }
}

def generateTrainTestSplit(allData: RDD[LabeledWindow], holdOutCellTypes: Set[CellTypes.Value],
 holdOutChromosomes: Set[String]) = {
        val train = allData.filter { window => !holdOutChromosomes.contains(window.win.getRegion.referenceName) && !holdOutCellTypes.contains(window.win.cellType) }
          .setName("train").cache()
        val test = allData.filter { window => holdOutChromosomes.contains(window.win.getRegion.referenceName) && holdOutCellTypes.contains(window.win.cellType) }
          .setName("test").cache()
        (train, test)
 }
}
