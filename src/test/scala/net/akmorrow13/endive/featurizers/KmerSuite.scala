package net.akmorrow13.endive.featurizers

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.{CellTypes, TranscriptionFactors}
import net.akmorrow13.endive.processing.{Sequence, Preprocess}
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
    val kmers = Kmer.generateKmers(2)

    assert(kmers.length == Math.pow(bases, 2))
    assert(kmers.length == kmers.distinct.length)
  }

  test("gets all kmers of length 3") {
    val kmers = Kmer.generateKmers(3)

    assert(kmers.length == Math.pow(bases, 3))
    assert(kmers.length == kmers.distinct.length)
  }

  sparkTest("should extract kmers using reference and regions") {
    val trainRDD: RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)] = Preprocess.loadLabels(sc, labelPath)._1

    val reference = Sequence(sc.parallelize(Seq(fragment)), sc)
    val sequences: RDD[(ReferenceRegion, String)] = reference.extractSequences(trainRDD.map(_._3))
    val kmers = Kmer.extractKmers(sequences.map(_._2), 8)

    val kmerCounts = Kmer.generateKmers(8).length

    assert(kmers.first.size == kmerCounts)

  }

  sparkTest("should extract kmers with differnece of 1") {
    var str1 = "AAAAAAAAATAAAAAA"
    val differences = 1
    val kmers = Kmer.generateKmers(8)
    var rdd = Kmer.extractKmers(sc.parallelize(Seq(str1)), 8, differences)
    var first = rdd.first.toArray.zip(kmers).filter(_._1 > 0.0)
    assert(first.apply(0)._1 == 9.0) //"AAAAAAAA" should occur 9 times with 1 difference

    str1 = "AAAAAAAAATTAAAAA"
    rdd = Kmer.extractKmers(sc.parallelize(Seq(str1)), 8, differences)
    first = rdd.first.toArray.zip(kmers).filter(_._1 > 0.0)
    assert(first.apply(0)._1 == 3.0) //"AAAAAAAA" should occur 3 times with 1 difference

  }


}
