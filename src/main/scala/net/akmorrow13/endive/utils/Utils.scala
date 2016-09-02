package net.akmorrow13.endive.utils

import net.akmorrow13.endive.processing.{Dataset, TranscriptionFactors, CellTypes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
import scala.reflect.ClassTag
import scala.util.Random

object EndiveUtils {

  val DEFAULTSEED = 0
  val DEFAULTSAMPLING = 0.001

/* Generate folds RDD */
def generateFoldsRDD[T: ClassTag](allData:RDD[((String, CellTypes.Value), T)], numHeldOutCellTypes: Int = 1, numHeldOutChromosomes: Int = 3, numFolds: Int = 10, sampleFreq: Option[Double] = Some(DEFAULTSAMPLING), randomSeed:Int = DEFAULTSEED) = {

    @transient
    val r = new Random(randomSeed)

    val sampledData =
      if (sampleFreq.isDefined)
        allData.sample(false, sampleFreq.get, randomSeed)
      else
        allData


    val cellTypesChromosomes:Set[(String, CellTypes.Value)] = sampledData.map(x => x._1).distinct().collect.toSet

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


  /**
   * Filters negative samples close to true peaks with open chromatin
   * @param sc
   * @param rdd
   * @param distance
   * @return
   */
  def subselectSamples(sc: SparkContext,
                       rdd: RDD[LabeledWindow],
                       sd: SequenceDictionary,
                       distance: Long = 700L,
                       partition: Boolean = true): RDD[LabeledWindow] = {
    val partitionedRDD: RDD[((ReferenceRegion, TranscriptionFactors.Value), LabeledWindow)] =
      if (partition)
        rdd.keyBy(r => (r.win.getRegion, r.win.getTf)).partitionBy(GenomicRegionPartitioner(Dataset.partitions, sd))
      else rdd.keyBy(r => (r.win.getRegion, r.win.getTf))

    partitionedRDD.mapPartitions(iter => {
      val sites = iter.toList
      val positives = sites.filter(r => r._2.label == 1.0)
      val negatives = sites.filter(r => r._2.label == 0.0)
      val minimum = 200L

      // favor negative samples closer to positive samples with open chromatin
      val filteredNegs = negatives.filter(n => {
        val neighbors = positives.filter(p => {
          val dist = n._2.win.getRegion.distance(p._2.win.getRegion)
          dist.isDefined && dist.get < distance && dist.get > minimum && p._2.win.getTf == n._2.win.getTf && p._2.win.getCellType == n._2.win.getCellType
        })
        !neighbors.isEmpty
      })
      filteredNegs.union(positives).toIterator
    }).map(_._2)
  }

  def subselectRandomSamples(sc: SparkContext,
                             rdd: RDD[LabeledWindow],
                             sd: SequenceDictionary,
                             sampleFreq: Double = DEFAULTSAMPLING,
                             randomSeed: Int = DEFAULTSEED): RDD[LabeledWindow] = {

    val positives = rdd.filter(_.label > 0)
    val negatives = rdd.filter(_.label == 0).sample(false, sampleFreq, randomSeed)
    positives.union(negatives)
  }

}
