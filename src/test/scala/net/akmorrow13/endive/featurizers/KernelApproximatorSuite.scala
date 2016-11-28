package net.akmorrow13.endive.featurizers

import java.io._

import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.Dataset
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.util.ClassLabelIndicatorsFromIntLabels
import org.apache.commons.math3.random.MersenneTwister
import utils.Stats
import scala.io.Source

class KernelApproximatorSuite extends EndiveFunSuite with Serializable {

  // fragment used for reference
  val sequenceLong = "TTGGAAAGAGGACGTGGGACTGGGATTTACTCGGCCACCAAAACACTCAC" * 4
  val sequenceLong2 = "TATCGTTTACGAGTATATTTTTTAAAGGCTCTCTCATAGAATACTGGGAC" * 4
  val sequenceShort = "ATCG"
  val alphabetSize = Dataset.alphabet.size
  val seed = 0
  var trainPRC = 0.0
  var testPRC = 0.0
  var trainROC = 0.0
  var testROC = 0.0
  var Wtest: DenseMatrix[Double] = null

  // accepted error for difference between python answers
  val error = 0.0000000001


  test("Test make ngrams") {
    /* This should yield a vector of length 16 */
    val ngramSize = 1
    val sequenceVector:DenseVector[Double] = KernelApproximator.stringToVector(sequenceShort)
    val outSize = sequenceShort.length - ngramSize + 1
    val expected = Array("A","T","C","G")

    val ngrams = KernelApproximator.makeNgrams(sequenceVector, ngramSize)
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
    val ngrams = KernelApproximator.makeNgrams(sequenceVector, ngramSize)
    val ngramStrings = KernelApproximator.vectorsToStrings(ngrams)
    assert(ngramStrings.deep == expected.deep)
  }

  test("Test if makeNgrams looks correct, for 8grams") {
    val ngramSize = 8
    val seqSize = sequenceLong.size
    val sequenceVector:DenseVector[Double] = KernelApproximator.stringToVector(sequenceLong)
    val outSize = seqSize - ngramSize + 1
    val expected = Array("TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC", "CACTCACT", "ACTCACTT", "CTCACTTG", "TCACTTGG", "CACTTGGA", "ACTTGGAA", "CTTGGAAA", "TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC", "CACTCACT", "ACTCACTT", "CTCACTTG", "TCACTTGG", "CACTTGGA", "ACTTGGAA", "CTTGGAAA", "TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC", "CACTCACT", "ACTCACTT", "CTCACTTG", "TCACTTGG", "CACTTGGA", "ACTTGGAA", "CTTGGAAA", "TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC")
    val ngrams = KernelApproximator.makeNgrams(sequenceVector, ngramSize)
    val ngramStrings = KernelApproximator.vectorsToStrings(ngrams)
    assert(ngramStrings.deep == expected.deep)
  }

  test("Test kernel approx for linear case") {
    val ngramSize = 8
    val seqSize = sequenceLong.size
    val outSize = seqSize - ngramSize + 1
    val approxDim = 4000
    val sequenceVector:DenseVector[Double] = KernelApproximator.stringToVector(sequenceLong)
    val sequenceVector2:DenseVector[Double] = KernelApproximator.stringToVector(sequenceLong2)


    val ngrams1 = KernelApproximator.makeNgrams(sequenceVector, ngramSize)
    val ngrams2 = KernelApproximator.makeNgrams(sequenceVector2, ngramSize)
    ngrams1 :/= KernelApproximator.computeConvolutionalNorm(ngrams1)
    ngrams2 :/= KernelApproximator.computeConvolutionalNorm(ngrams2)

    /* Linear kernel is just the dot product */
    var kxy = 0.0
    var kxx = 0.0
    var kyy = 0.0

    var i = 0
    while (i < outSize) {
      var j = 0
      while (j < outSize) {
        val ngram1:DenseVector[Double] = ngrams1(i,::).t
        val ngram2:DenseVector[Double] = ngrams2(j,::).t
        val k = (ngram1.t * ngram2)
        kxy += k
        j += 1
      }
      i += 1
    }

    i = 0
    while (i < outSize) {
      var j = 0
      while (j < outSize) {
        val ngram1:DenseVector[Double] = ngrams1(i,::).t
        val ngram2:DenseVector[Double] = ngrams1(j,::).t
        val k = (ngram1.t * ngram2)
        kxx += k
        j += 1
      }
      i += 1
    }

    i = 0
    while (i < outSize) {
      var j = 0
      while (j < outSize) {
        val ngram1:DenseVector[Double] = ngrams2(i,::).t
        val ngram2:DenseVector[Double] = ngrams2(j,::).t
        val k = (ngram1.t * ngram2)
        kyy += k
        j += 1
      }
      i += 1
    }

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)
    val W = DenseMatrix.rand(approxDim, ngramSize*alphabetSize, gaussian)
    val kernelApprox = new KernelApproximator(W)
    var Wx = kernelApprox(sequenceVector)
    var Wy = kernelApprox(sequenceVector2)
    Wx :/= norm(Wx)
    Wy :/= norm(Wy)
    val kxyhat = (Wx.t * Wy)
    val kxxhat = (Wx.t * Wx)
    val kyyhat = (Wy.t * Wy)

    assert(Stats.aboutEq(kxyhat, kxy, 0.01))
    assert(Stats.aboutEq(kxxhat, kxx, 0.01))
    assert(Stats.aboutEq(kyyhat, kyy, 0.01))

  }

  sparkTest("Testing that output is same as paper results") {

    val W = breeze.linalg.csvread(new File(getClass.getResource("nprandom_4000_32.csv").getPath))
    assert(W.size == 128000)

    Wtest = W
    val pythonScriptOutput = breeze.linalg.csvread(new File(getClass.getResource("seq.features").getPath)).toDenseVector

    val infile = sc.textFile(getClass.getResource("EGR1_withNegatives/EGR1_GM12878_Egr-1_HudsonAlpha_AC.seq.100Lines").getPath).filter(f => f(0) == 'A')

    assert(infile.count == 98)
    val train = infile.map(f => f.split(" ")).map(f => (f(2), f.last.toInt))

    val ngramSize = 8
    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))

    val kernelApprox = new KernelApproximator(W, Math.cos, ngramSize = ngramSize)

    val trainApprox = train.map(f => (KernelApproximator.stringToVector(f._1), f._2))

    //98 records after 2 lines at top for column names
    assert(trainApprox.count == 98)
    //verify that the two outputs are extremely similar given the same random matrix
    assert(norm(trainApprox.first._1) - norm(pythonScriptOutput) < error)
  }

}
