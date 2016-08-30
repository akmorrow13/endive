package net.akmorrow13.endive.utils

import net.akmorrow13.endive.processing.CellTypes
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.util.Random

object EndiveUtils {

/* Generate folds RDD */
def generateFoldsRDD[T: ClassTag](allData:RDD[((String, CellTypes.Value), T)], numHeldOutCellTypes: Int = 1, numHeldOutChromosomes: Int = 3, numFolds: Int = 10, sampleFreq: Double = 0.001, randomSeed:Int = 0) = {

    @transient
    val r = new Random(randomSeed)

    val sampledData = allData.sample(false, sampleFreq)

    val cellTypesChromosomes:Set[(String, CellTypes.Value)] = sampledData.map(x => x._1).countByValue().keys.toSet

    /* this will work with exponentially high probability */
    val cellTypes:Iterable[CellTypes.Value] = cellTypesChromosomes.map(_._2)

    /* this will work with exponentially high probability */
    val chromosomes:Iterable[String] = cellTypesChromosomes.map(_._1)

    for (i <- (0 until numFolds)) yield
        {
        val holdOutCellTypes = r.shuffle(cellTypes).take(numHeldOutCellTypes).toSet
        val holdOutChromosomes = r.shuffle(chromosomes).take(numHeldOutChromosomes).toSet
        generateTrainTestSplit(allData, holdOutCellTypes, holdOutChromosomes)
        }
}

def generateTrainTestSplit[T: ClassTag](allData: RDD[((String, CellTypes.Value), T)], holdOutCellTypes: Set[CellTypes.Value],
 holdOutChromosomes: Set[String]) = {
        val train = allData.filter { window => !holdOutChromosomes.contains(window._1._1) && !holdOutCellTypes.contains(window._1._2) }
          .setName("train").cache()
        val test = allData.filter { window => holdOutChromosomes.contains(window._1._1) && holdOutCellTypes.contains(window._1._2) }
          .setName("test").cache()
        (train, test)
 }
}
