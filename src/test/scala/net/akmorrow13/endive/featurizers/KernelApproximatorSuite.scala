package net.akmorrow13.endive.featurizers

import java.io.File

import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.pipelines.DnaseKernelPipeline
import org.apache.commons.math3.random.MersenneTwister
import net.akmorrow13.endive.processing.Dataset

import nodes.akmorrow13.endive.featurizers.KernelApproximator
import utils.Stats

class KernelApproximatorSuite extends EndiveFunSuite with Serializable {

  // fragment used for reference
  val sequenceLong = "TTGGAAAGAGGACGTGGGACTGGGATTTACTCGGCCACCAAAACACTCAC" * 4
  val sequenceLong2 = "TATCGTTTACGAGTATATTTTTTAAAGGCTCTCTCATAGAATACTGGGAC" * 4
  val sequenceShort = "ATCG"
  val alphabetSize = Dataset.alphabet.size
  val seed = 0

  // accepted error for difference between python answers
  val error = 0.0000000001


  test("Test make ngrams") {
    /* This should yield a vector of length 16 */
    val ngramSize = 1
    val sequenceVector:DenseVector[Double] = KernelApproximator.stringToVector(sequenceShort)
    val outSize = sequenceShort.length - ngramSize + 1
    val expected = Array("A","T","C","G")

    val ngrams = KernelApproximator.makeNgrams(sequenceVector, ngramSize, 4)
    val ngramStrings = KernelApproximator.vectorsToStrings(ngrams)
    assert(ngramStrings.deep == expected.deep)
  }

  test("Test if makeNgrams looks correct, for 2grams") {
    /* This should yield a vector of length 16 */
    val ngramSize = 2
    val seqSize = sequenceShort.size
    val sequenceVector:DenseVector[Double] = KernelApproximator.stringToVector(sequenceShort)
    val outSize = seqSize - ngramSize + 1
    val expected = Array("AT","TC","CG")
    val ngrams = KernelApproximator.makeNgrams(sequenceVector, ngramSize, 4)
    val ngramStrings = KernelApproximator.vectorsToStrings(ngrams)
    assert(ngramStrings.deep == expected.deep)
  }

  test("Test if makeNgrams looks correct, for 8grams") {
    val ngramSize = 8
    val seqSize = sequenceLong.size
    val sequenceVector:DenseVector[Double] = KernelApproximator.stringToVector(sequenceLong)
    val outSize = seqSize - ngramSize + 1
    val expected = Array("TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC", "CACTCACT", "ACTCACTT", "CTCACTTG", "TCACTTGG", "CACTTGGA", "ACTTGGAA", "CTTGGAAA", "TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC", "CACTCACT", "ACTCACTT", "CTCACTTG", "TCACTTGG", "CACTTGGA", "ACTTGGAA", "CTTGGAAA", "TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC", "CACTCACT", "ACTCACTT", "CTCACTTG", "TCACTTGG", "CACTTGGA", "ACTTGGAA", "CTTGGAAA", "TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC")
    val ngrams = KernelApproximator.makeNgrams(sequenceVector, ngramSize, 4)
    val ngramStrings = KernelApproximator.vectorsToStrings(ngrams)
    assert(ngramStrings.deep == expected.deep)
  }

  sparkTest("Testing that output is same as paper results") {

    val W = breeze.linalg.csvread(new File(resourcePath("nprandom_4000_32.csv")))
    assert(W.size == 128000)
    val pythonScriptOutput = breeze.linalg.csvread(new File(resourcePath("seq.features"))).toDenseVector

    val sequence = sc.textFile(resourcePath("EGR1_withNegatives/EGR1_GM12878_Egr-1_HudsonAlpha_AC.seq.100Lines")).filter(f => f(0) == 'A').first
      .split(" ")(2)

    val ngramSize = 8
    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)


    val featurized = DnaseKernelPipeline.featurizeString(sequence, Array(W), Array(8))

    assert(norm(featurized) - norm(pythonScriptOutput) < error)
  }
}
