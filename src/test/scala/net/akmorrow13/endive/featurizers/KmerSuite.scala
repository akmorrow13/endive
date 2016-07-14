package net.akmorrow13.endive.featurizers

import net.akmorrow13.endive.{Endive, EndiveFunSuite}
import net.akmorrow13.endive.processing.Sequence
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}

class KmerSuite extends EndiveFunSuite {

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

  test("gets all kmers of length 2") {
    val kmers = Kmer.generateAllKmers(2)

    assert(kmers.length == Math.pow(bases, 2))
    assert(kmers.length == kmers.distinct.length)
  }

  test("gets all kmers of length 3") {
    val kmers = Kmer.generateAllKmers(3)

    assert(kmers.length == Math.pow(bases, 3))
    assert(kmers.length == kmers.distinct.length)
  }

  sparkTest("should extract kmers using reference and regions") {
    val trainRDD = Endive.loadTsv(sc, labelPath)

    val reference = Sequence(sc.parallelize(Seq(fragment)), sc)
    val sequences: RDD[(ReferenceRegion, String)] = reference.extractSequences(trainRDD.map(_._1))
    val kmers = Kmer.extractKmers(sequences, 8)

    val kmerCounts = Kmer.generateAllKmers(8).length

    assert(kmers.first.size == kmerCounts)

  }

}
