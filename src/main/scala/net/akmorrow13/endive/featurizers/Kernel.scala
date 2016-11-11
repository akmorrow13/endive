package nodes.akmorrow13.endive.featurizers

import breeze.linalg._
import nodes.util.ClassLabelIndicatorsFromIntLabels
import org.apache.spark.rdd.RDD
import workflow.Transformer
import breeze.numerics._
import net.akmorrow13.endive.processing.Dataset



/* This only works for small alphabet size, use sparse matrix later */
class KernelApproximator(filters: DenseMatrix[Double],
                         nonLin: Double => Double = (x: Double) => x ,
                         offset:Option[DenseVector[Double]] = None,
                         ngramSize: Int = 8,
                         alphabetSize: Int = Dataset.bases.size,
                         seqSize: Int = Dataset.windowSize)
  extends Transformer[DenseVector[Double], DenseVector[Double]] with Serializable {


  /* valid convolution */
  val outSize = seqSize - ngramSize + 1

  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    in.mapPartitions(convolvePartitions(_, filters, nonLin, offset, ngramSize, outSize, alphabetSize))
  }

  def apply(in: DenseVector[Double]): DenseVector[Double]= {
    val ngramMat = null//new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
    convolve(in, ngramMat, filters, nonLin, offset)
  }

  def convolve(seq: DenseVector[Double],
    ngramMat: DenseMatrix[Double],
    filters: DenseMatrix[Double],
    nonLin: Double => Double,
    offset: Option[DenseVector[Double]],
    alphabetSize: Int = 4): DenseVector[Double] = {

    /* Make the ngram */
    val ngrams: DenseMatrix[Double] = KernelApproximator.makeNgrams(seq, ngramMat, ngramSize)
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
    alphabetSize: Int = 4): Iterator[DenseVector[Double]] = {
      val ngramMat = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
      seq.map(convolve(_, ngramMat, filters, nonLin, offset, alphabetSize))
    }
  }

object KernelApproximator  {


  def denseFeaturize(in: String): DenseVector[Double] = {
    /* Identity featurizer */

    val sequenceVectorizer = ClassLabelIndicatorsFromIntLabels(4)

    val intString:Seq[Int] = in.map(Dataset.bases(_))
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
    val alphabetSize = Dataset.alphabet.size
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

  def makeNgrams(seq: DenseVector[Double],
                  ngramMat: DenseMatrix[Double],
                  ngramSize: Int,
                  alphabetSize: Int = 4): DenseMatrix[Double] = {

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
