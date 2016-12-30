package nodes.akmorrow13.endive.featurizers

import breeze.linalg._
import nodes.util.ClassLabelIndicatorsFromIntLabels
import org.apache.spark.rdd.RDD
import workflow.Transformer
import breeze.numerics._
import net.akmorrow13.endive.processing.Dataset

class KernelApproximator(filters: DenseMatrix[Double],
                         nonLin: Double => Double = (x: Double) => x ,
                         ngramSize: Int,
                         alphabetSize: Int,
                         offset:Option[DenseVector[Double]] = None,
                         seqSize: Int = Dataset.windowSize)
  extends Transformer[DenseVector[Double], DenseVector[Double]] with Serializable {


  /* valid convolution */
  val outSize = seqSize - ngramSize + 1
   println(s"mapping partitions alphsize: ${alphabetSize}, outsize: ${outSize}, ngramsize: ${ngramSize}")

  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    in.mapPartitions(convolvePartitions(_, filters, nonLin, offset, ngramSize, outSize, alphabetSize))
  }

  def apply(in: DenseVector[Double]): DenseVector[Double]= {
    convolve(in, filters, nonLin, offset, alphabetSize)
  }

  def convolve(seq: DenseVector[Double],
    filters: DenseMatrix[Double],
    nonLin: Double => Double,
    offset: Option[DenseVector[Double]],
    alphabetSize: Int): DenseVector[Double] = {

    /* Make the ngram */
    val ngrams: DenseMatrix[Double] = KernelApproximator.makeNgrams(seq, ngramSize, alphabetSize)

    println("NGRAM SIZE " + ngrams.rows + "," + ngrams.cols + "\n")
    println("FILTER SIZE " + filters.rows + "," + filters.cols + "\n")

    /* Actually do the convolution */
    val convRes: DenseMatrix[Double] = ngrams * filters.t

    /* Apply non linearity element wise */
    var i = 0
    while (i < convRes.rows) {
      var j = 0
      while (j < convRes.cols) {
        val phase = offset.map(x => x(i)).getOrElse(0.0)
        convRes(i,j) = nonLin(convRes(i,j) + phase)
        j += 1
      }
     i += 1
    }

    /* sum across spatial dimension */
    val outV =  sum(convRes, Axis._0).toDenseVector
    outV *= sqrt(2) * 1.0/sqrt(filters.rows)

    outV
  }

  def convolvePartitions(seq: Iterator[DenseVector[Double]],
    filters: DenseMatrix[Double],
    nonLin: Double => Double,
    offset:Option[DenseVector[Double]],
    ngramSize: Int,
    outSize: Int,
    alphabetSize: Int): Iterator[DenseVector[Double]] = {
      val ngramMat = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
      seq.map(convolve(_, filters, nonLin, offset, alphabetSize))
    }
  }


/**
 * Helper functions for kernel approximation
 */
object KernelApproximator  {

  /**
   * Converts matrix of ngrams (densevectors of length alphabetsize * stringlength)
   * to strings
   * @param ngramMat matrix of densevectors, where each row encodes a string
   * @return array of strings, where each string is from a row in ngramMat
   */
  def vectorsToStrings(ngramMat: DenseMatrix[Double]): Array[String] =  {
    val ngramStrings:Array[String] = new Array[String](ngramMat.rows)

    Array.range(0, ngramMat.rows).map(i => {
      ngramStrings(i) = vectorToString(ngramMat(i, ::).t.toDenseVector)
    })

    ngramStrings
  }

  /**
   * Converts string of length l to 4xl length densevector
   *
   * @param in string to convert
   * @return DenseVector encoding string
   */
  def stringToVector(in: String): DenseVector[Double] = {
    /* Identity featurizer */

    val sequenceVectorizer = ClassLabelIndicatorsFromIntLabels(4)

    val intString:Seq[Int] = in.map(r => Dataset.alphabet.get(r).getOrElse(-1))
    val seqString = intString.map { bp =>
      val out = DenseVector.zeros[Double](4)
      if (bp != -1) {
        out(bp) = 1
      }
      out
    }
    DenseVector.vertcat(seqString:_*)
  }

  /**
   * Conversts a 4*b length vector to an string of A,T,G,C,N
   * Node that each 4 doubles encodes 1 Char, eg 0.0 0.0 0.0 0.0 => N
   *
   * @param in DenseVector encoding sequence string
   * @return DNA string
   */
  def vectorToString(in: DenseVector[Double]): String = {

    val BASEPAIRREVMAP = Dataset.alphabet.map(_._1).toArray

    val alphabetSize = Dataset.alphabet.size // number of bases
    val stringSize = in.size / alphabetSize  // final size of string
    var i = 0
    val strArr = new Array[Char](stringSize)

    Array.range(0, stringSize)
      .map(i => {
        val charVector = in(i*alphabetSize until i*alphabetSize+alphabetSize)
        if (charVector == DenseVector.zeros[Double](alphabetSize)) {
          strArr(i) = 'N'
        } else {
          val bp = BASEPAIRREVMAP(argmax(charVector))
          strArr(i) = bp
        }
      })
    strArr.mkString
  }

  /**
   * Computes aggregate norm over matrix
   *
   * @param X Dense Matrix
   * @return final norm
   */
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

  /**
   * Computes ngrams from sequence
   *
   * @param seq densevector encoding string
   * @param ngramSize size of ngrams
   * @param alphabetSize number of characters in alphabet (default is 4)
   * @return ngrams
   */
  def makeNgrams(seq: DenseVector[Double],
                  ngramSize: Int,
                  alphabetSize: Int): DenseMatrix[Double] = {

    /* The length of seq is alphabet size times sequence length */
    val numSymbols = seq.size/alphabetSize

    /* valid convolution */
    val outSize = numSymbols - ngramSize + 1
    val ngramMat = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
    var i = 0
    while (i < outSize) {
      val currNgram = seq(i*alphabetSize until (i + ngramSize)*alphabetSize)
      for(j <- 0 until currNgram.length) {
        ngramMat(i,j) = currNgram(j)
      }
      i += 1
    }
    ngramMat
  }
}
