package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}

class SequenceSuite extends EndiveFunSuite {

  val bases = 4
  val chr = "chr10"

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")

  // fragment used for reference
  val sequence = "TTGGAAAGAGGACGTGGGACTGGGATTTACTCGGCCACCAAAACACTCAC" * 200;
  val fragment = NucleotideContigFragment.newBuilder()
    .setContig(Contig.newBuilder().setContigName(chr).build())
    .setFragmentSequence(sequence)
    .setFragmentStartPosition(1L)
    .setFragmentEndPosition(10000L)
    .build()

  sparkTest("should extract reference sequences using reference and regions") {
    val trainRDD = Preprocess.loadLabels(sc, labelPath)._1
    // assert tsv loader only loads unbould labels
    assert(trainRDD.count == 29)
    assert(trainRDD.filter(r => r._4 == -1).count() == 1)
    assert(trainRDD.filter(r => r._4 == 1).count() == 1)

    val reference = Sequence(sc.parallelize(Seq(fragment)), sc)
    val sequences: RDD[(ReferenceRegion, String)] = reference.extractSequences(trainRDD.map(_._3))

    assert(sequences.count == trainRDD.count)

  }
}
