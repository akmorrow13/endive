package net.akmorrow13.endive.featurizers

import breeze.linalg._
import breeze.numerics._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.{CellTypes, TranscriptionFactors, Preprocess, Sequence}
import nodes.util.{Identity, Cacher, ClassLabelIndicatorsFromIntLabels, TopKClassifier, MaxClassifier, VectorCombiner}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
import org.apache.commons.math3.random.MersenneTwister

import nodes.akmorrow13.endive.featurizers.KernelApproximator
import utils.Stats

class KernelApproximatorSuite extends EndiveFunSuite {

  val bases = 4

  // fragment used for reference
  val sequenceLong = "TTGGAAAGAGGACGTGGGACTGGGATTTACTCGGCCACCAAAACACTCAC" * 4
  val sequenceLong2 = "TATCGTTTACGAGTATATTTTTTAAAGGCTCTCTCATAGAATACTGGGAC" * 4
  val sequenceShort = "ATCG"
  val alphabetSize = 4
  val seed = 0


  def denseFeaturize(in: String): DenseVector[Double] = {
    /* Identity featurizer */

   val BASEPAIRMAP = Map('N'-> -1, 'A' -> 0, 'T' -> 1, 'C' -> 2, 'G' -> 3)
    val sequenceVectorizer = ClassLabelIndicatorsFromIntLabels(4)

    val intString:Seq[Int] = in.map(BASEPAIRMAP(_))
    val seqString = intString.map { bp =>
      val out = DenseVector.zeros[Double](4)
      if (bp != -1) {
        out(bp) = 1
      }
      out
    }
    DenseVector.vertcat(seqString:_*)
  }

  def vectorToString(in: DenseVector[Double]): String = {

   val BASEPAIRREVMAP = Array('A', 'T', 'C', 'G')
   var i = 0
   var str = ""
   while (i < in.size) {
    val charVector = in(i until i+alphabetSize)
    if (charVector == DenseVector.zeros[Double](alphabetSize)) {
      str += "N"
    } else {
      val bp = BASEPAIRREVMAP(argmax(charVector))
      str += bp
    }
    i += alphabetSize
   }
   str
  }

  def computeConvolutionalNorm(X: DenseMatrix[Double]): Double =  {
    var i = 0
    var norm = 0.0
    while (i < X.rows) {
      var j = 0
      while (j < X.rows) {
        val ngram1:DenseVector[Double] = X(i,::).t
        val ngram2:DenseVector[Double] = X(j,::).t
        val k = (ngram1.t * ngram2)
        norm += k
        j += 1
      }
      i += 1
    }
    sqrt(norm)
  }

  def convertNgramsToStrings(ngramMat: DenseMatrix[Double], outSize:Int ): Array[String] =  {
    var i = 0
    val ngramStrings:Array[String] = new Array[String](outSize)
    while (i < outSize) {
      val ngramString = vectorToString(ngramMat(i, ::).t.toDenseVector)
      ngramStrings(i) = ngramString
      i += 1
    }
    ngramStrings
  }



  test("Test if makeNgrams looks correct") {
    /* This should yield a vector of length 16 */
    val ngramSize = 1
    val seqSize = sequenceShort.size
    val sequenceVector:DenseVector[Double] = denseFeaturize(sequenceShort)
    val outSize = seqSize - ngramSize + 1
    val expected = Array("A","T","C","G")

    var ngramMat = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
    val ngrams = KernelApproximator.makeNgrams(sequenceVector, ngramMat, ngramSize,  alphabetSize)
    val ngramStrings = convertNgramsToStrings(ngrams, outSize)
    println(s"INPUT SEQUENCE: ${sequenceShort}, NGRAM SIZE: ${ngramSize}")
    println(s"NGRAMS: ${ngramStrings.mkString(",")}")
    assert(ngramStrings.deep == expected.deep)
  }

  test("Test if makeNgrams looks correct, for 2grams") {
    /* This should yield a vector of length 16 */
    val ngramSize = 2
    val seqSize = sequenceShort.size
    val sequenceVector:DenseVector[Double] = denseFeaturize(sequenceShort)
    val outSize = seqSize - ngramSize + 1
    val expected = Array("AT","TC","CG")
    var ngramMat = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
    val ngrams = KernelApproximator.makeNgrams(sequenceVector, ngramMat, ngramSize,  alphabetSize)
    val ngramStrings = convertNgramsToStrings(ngrams, outSize)
    println(s"INPUT SEQUENCE: ${sequenceShort}, NGRAM SIZE: ${ngramSize}")
    println(s"NGRAMS: ${ngramStrings.mkString(",")}")
    assert(ngramStrings.deep == expected.deep)
  }

  test("Test if makeNgrams looks correct, for 8grams") {
    val ngramSize = 8
    val seqSize = sequenceLong.size
    val sequenceVector:DenseVector[Double] = denseFeaturize(sequenceLong)
    val outSize = seqSize - ngramSize + 1
    val expected = Array("TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC", "CACTCACT", "ACTCACTT", "CTCACTTG", "TCACTTGG", "CACTTGGA", "ACTTGGAA", "CTTGGAAA", "TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC", "CACTCACT", "ACTCACTT", "CTCACTTG", "TCACTTGG", "CACTTGGA", "ACTTGGAA", "CTTGGAAA", "TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC", "CACTCACT", "ACTCACTT", "CTCACTTG", "TCACTTGG", "CACTTGGA", "ACTTGGAA", "CTTGGAAA", "TTGGAAAG", "TGGAAAGA", "GGAAAGAG", "GAAAGAGG", "AAAGAGGA", "AAGAGGAC", "AGAGGACG", "GAGGACGT", "AGGACGTG", "GGACGTGG", "GACGTGGG", "ACGTGGGA", "CGTGGGAC", "GTGGGACT", "TGGGACTG", "GGGACTGG", "GGACTGGG", "GACTGGGA", "ACTGGGAT", "CTGGGATT", "TGGGATTT", "GGGATTTA", "GGATTTAC", "GATTTACT", "ATTTACTC", "TTTACTCG", "TTACTCGG", "TACTCGGC", "ACTCGGCC", "CTCGGCCA", "TCGGCCAC", "CGGCCACC", "GGCCACCA", "GCCACCAA", "CCACCAAA", "CACCAAAA", "ACCAAAAC", "CCAAAACA", "CAAAACAC", "AAAACACT", "AAACACTC", "AACACTCA", "ACACTCAC")
    var ngramMat = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
    val ngrams = KernelApproximator.makeNgrams(sequenceVector, ngramMat, ngramSize,  alphabetSize)
    val ngramStrings = convertNgramsToStrings(ngrams, outSize)
    println(ngramStrings.size)
    println(s"INPUT SEQUENCE: ${sequenceShort}, NGRAM SIZE: ${ngramSize}")
    println(s"NGRAMS: ${ngramStrings.mkString(",")}")
    assert(ngramStrings.deep == expected.deep)
  }

  test("Test kernel approx for linear case") {
    val ngramSize = 8
    val seqSize = sequenceLong.size
    val outSize = seqSize - ngramSize + 1
    val approxDim = 4000
    val sequenceVector:DenseVector[Double] = denseFeaturize(sequenceLong)
    val sequenceVector2:DenseVector[Double] = denseFeaturize(sequenceLong2)

    var ngramMat1 = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
    var ngramMat2 = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)


    val ngrams1 = KernelApproximator.makeNgrams(sequenceVector, ngramMat1, ngramSize,  alphabetSize)
    val ngrams2 = KernelApproximator.makeNgrams(sequenceVector2, ngramMat2, ngramSize,  alphabetSize)
   ngrams1 :/= computeConvolutionalNorm(ngrams1)
   ngrams2 :/= computeConvolutionalNorm(ngrams2)

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
    val Wx = kernelApprox(sequenceVector)
    val Wy = kernelApprox(sequenceVector2)

    val kxyhat = (Wx.t * Wy)
    val kxxhat = (Wx.t * Wx)
    val kyyhat = (Wy.t * Wy)

    assert(Stats.aboutEq(kxyhat, kxy, 0.01))
    assert(Stats.aboutEq(kxxhat, kxx, 0.01))
    assert(Stats.aboutEq(kyyhat, kyy, 0.01))


  }
}
