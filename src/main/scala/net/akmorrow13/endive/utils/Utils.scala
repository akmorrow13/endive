package net.akmorrow13.endive.utils

import org.apache.spark.rdd.RDD
import Crossable.trav2Crossable

object EndiveUtils {

/* Generate folds RDD */
def generateFoldsRDD(allData:RDD[LabeledWindow], cellTypesPerFold: Int = 1, chromosomesPerFold: Int = 11, sampleFreq: Double = 0.001 ) = {

    /* this will work with exponentially high probability */
    val cellTypes:Iterable[String] = allData.sample(false, sampleFreq).map(x => (x.win.cellType)).countByValue().keys

    val cellTypesMap = cellTypes.zipWithIndex.toMap

    /* this will work with exponentially high probability */
    val chromosomes:Iterable[String] = allData.sample(false, sampleFreq).map(x => (x.win.getRegion.referenceName)).countByValue().keys

    val chromosomesMap = chromosomes.zipWithIndex.toMap

    val numChromesomeFolds: Int = chromosomes.size/chromosomesPerFold
    val numCellTypeFolds : Int = cellTypes.size/chromosomesPerFold

    val numFolds = numChromesomeFolds * numCellTypeFolds

    val cellTypesChromesomes:Set[(String, String)] = (chromosomes × cellTypes).toSet

    for (i <- (0 until numFolds)) yield
        {
        val trainSet = cellTypesChromesomes.filter { x =>
                                chromosomesMap(x._1) % numFolds != i ||
                                cellTypesMap(x._2) % numFolds != i
                                }

        val testSet = cellTypesChromesomes.filter { x =>
                                chromosomesMap(x._1) % numFolds == i &&
                                cellTypesMap(x._2) % numFolds == i
                                }

        assert(trainSet.intersect(testSet).size == 0)

        val train = allData.filter { window =>
                                     trainSet.contains((window.win.getRegion.referenceName,  window.win.cellType))
                                    }
        val test = allData.filter { window =>
                                     testSet.contains((window.win.getRegion.referenceName,  window.win.cellType))
                                    }
        (train, test)
        }
}

}
