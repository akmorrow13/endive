package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}

class SequenceSuite extends EndiveFunSuite {

  val bases = 4
  val chr = "chr10"

  // training data of region and labels
  val train: Seq[(ReferenceRegion, Seq[Int])]  =
    Seq((ReferenceRegion(chr, 600, 800), Seq(0,0,0)),
      (ReferenceRegion(chr, 650, 850), Seq(0,0,0)),
      (ReferenceRegion(chr, 700, 900), Seq(0,0,0)),
      (ReferenceRegion(chr, 700, 900), Seq(0,0,0)),
      (ReferenceRegion(chr, 750, 950), Seq(0,0,0)),
      (ReferenceRegion(chr, 850, 1000), Seq(0,0,0)),
      (ReferenceRegion(chr, 1000, 1200), Seq(0,0,0)))

  // fragment used for reference
  val sequence = "TTGGAAAGAGGACGTGGGACTGGGATTTACTCGGCCACCAAAACACTCAC" * 200;
  val fragment = NucleotideContigFragment.newBuilder()
    .setContig(Contig.newBuilder().setContigName(chr).build())
    .setFragmentSequence(sequence)
    .setFragmentStartPosition(1L)
    .setFragmentEndPosition(10000L)
    .build()

  test("gets all kmers of length 2") {
    val kmers = Sequence.generateAllKmers(2)

    assert(kmers.length == Math.pow(bases, 2))
    assert(kmers.length == kmers.distinct.length)
  }

  test("gets all kmers of length 3") {
    val kmers = Sequence.generateAllKmers(3)

    assert(kmers.length == Math.pow(bases, 3))
    assert(kmers.length == kmers.distinct.length)
  }

  sparkTest("should extract reference sequences using reference and regions") {
    val reference = Sequence(sc.parallelize(Seq(fragment)), sc)
    val sequences: RDD[(ReferenceRegion, String, Seq[Int])] = reference.extractSequences(train)

    assert(sequences.count == train.length)

  }

  sparkTest("should extract kmers using reference and regions") {
    val reference = Sequence(sc.parallelize(Seq(fragment)), sc)
    val sequences: RDD[(ReferenceRegion, String, Seq[Int])] = reference.extractSequences(train)
    val kmers: RDD[MultiLabeledPoint] = Sequence.extractKmers(sequences, 8)

    assert(sequences.count == train.length)

  }

}
