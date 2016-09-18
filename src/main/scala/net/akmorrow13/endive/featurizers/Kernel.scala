package nodes.akmorrow13.endive.featurizers

import breeze.linalg._
import nodes.learning.ZCAWhitener
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pipelines._
import workflow.Transformer
import breeze.numerics._
import net.jafama.FastMath



/* This only works for small alphabet size, use sparse matrix later */
class KernelApproximator(filters: DenseMatrix[Double], nonLin: Double => Double = (x: Double) => x , ngramSize: Int = 8, alphabetSize: Int = 4, seqSize: Int = 200)
  extends Transformer[DenseVector[Double], DenseVector[Double]] {


  /* valid convolution */
  val outSize = seqSize - ngramSize + 1

  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    in.mapPartitions(convolvePartitions(_, filters, nonLin, ngramSize, outSize, alphabetSize))
  }

  def apply(in: DenseVector[Double]): DenseVector[Double]= {
    var ngramMat = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
    convolve(in, ngramMat, filters, nonLin)
  }

  def convolve(seq: DenseVector[Double],
    ngramMat: DenseMatrix[Double],
    filters: DenseMatrix[Double],
    nonLin: Double => Double,
    alphabetSize: Int = 4): DenseVector[Double] = {

      /* Make the ngram */
     var ngrams: DenseMatrix[Double] = KernelApproximator.makeNgrams(seq, ngramMat, ngramSize)

    /* Actually do the convolution */
    val convRes: DenseMatrix[Double] = ngrams * filters.t

    /* TODO: Do we want to normalize here?*/

    val convResArray = convRes.data

    /* Apply non linearity element wise */
   var i = 0
   while (i < convResArray.size) {
     convResArray(i) = nonLin(convResArray(i))
     i += 1
   }

   /* sum across spatial dimension */
  val outV = 1.0/sqrt(filters.rows) * sum(convRes, Axis._0).toDenseVector

  /* Normalize */
  outV :/= norm(outV)
  outV
  }


  def convolvePartitions(seq: Iterator[DenseVector[Double]],
    filters: DenseMatrix[Double],
    nonLin: Double => Double,
    ngramSize: Int,
    outSize: Int,
    alphabetSize: Int = 4): Iterator[DenseVector[Double]] =
    {
      var ngramMat = new DenseMatrix[Double](outSize, ngramSize*alphabetSize)
      seq.map(convolve(_, ngramMat, filters, nonLin, alphabetSize))
    }
  }

  object KernelApproximator  {

    def makeNgrams(seq: DenseVector[Double],
      ngramMat: DenseMatrix[Double],
      ngramSize: Int,
      alphabetSize: Int = 4): DenseMatrix[Double] = {

        /* The length of seq is alphabet size times sequence length */
       val numSymbols = seq.size/alphabetSize

       /* valid convolution */
    val outSize = numSymbols - ngramSize + 1

    var i = 0
    while (i < outSize) {
      val currNgram = seq(i*alphabetSize until (i + ngramSize)*alphabetSize)
      ngramMat(i, ::) := currNgram.t
      i += 1
    }
    ngramMat
  }

}
