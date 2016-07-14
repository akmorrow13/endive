package net.akmorrow13.endive.processing

import net.akmorrow13.endive.{Endive, EndiveFunSuite}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}

class SamplingSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")

  sparkTest("should subsample negative points close to positive points") {
    val trainRDD = Endive.loadTsv(sc, labelPath)
    val sampledRDD = Sampling.selectNegativeSamples(sc, trainRDD, 700L)
    val positives = sampledRDD.filter(_._2 == 1.0)
    val negatives = sampledRDD.filter(_._2 == 0.0)
    assert(negatives.count < positives.count * 20) // number of alloted sliding windows

  }
}
