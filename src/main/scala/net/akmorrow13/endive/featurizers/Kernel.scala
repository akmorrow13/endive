package nodes.images

import breeze.linalg._
import nodes.learning.ZCAWhitener
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import pipelines._
import workflow.Transformer


/* This only works for small alphabet size, use sparse matrix later */
class KernelApproximator(filters: DenseMatrix[Double], nonlin: Double => Double, ngramSize: Int = 8, alphabetSize: Int = 4, seqSize: Int = 200)
  extends Transformer[DenseVector[Double], DenseVector[Double]] {


  /* valid convolution */
  val outSize = seqSize - ngramSize + 1

  override def apply(in: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {
    in.mapPartitions(convolvePartitions(_, filters,ngramSize, outSize, alphabetSize))
  }

  def apply(in: DenseVector[Double]): DenseVector[Double]= {
    var ngramMat = new DenseMatrix[Double](outSize*alphabetSize, ngramSize)
    convolve(in, ngramMat, filters)
  }

  def convolve(seq: DenseVector[Double],
      ngramMat: DenseMatrix[Double],
      filters: DenseMatrix[Double],
      alphabetSize: Int = 4): DenseVector[Double] = {

    /* Make the ngram */

    val ngrams = makeNgrams(seq, ngramMat, ngramSize)

    /* Actually do the convolution */

    val convRes: DenseMatrix[Double] = seq * filters

    /* sum across spatial dimension */
    sum(convRes, Axis._1).toDenseVector
  }

  def makeNgrams(seq: DenseVector[Double],
      ngramMat: DenseMatrix[Double],
      ngramSize: Int,
      alphabetSize: Int = 4): DenseMatrix[Double] = {

    /* The length of seq is alphabet size times sequence length */
    val numSymbols = seq.size/alphabetSize

    /* valid convolution */
    val outSize = numSymbols - ngramSize + 1

    var i = 0
    while (i < alphabetSize*outSize) {
      ngramMat(::, *) := seq(i to i + alphabetSize*ngramSize)
      i += alphabetSize
    }
    ngramMat
  }

  def convolvePartitions(seq: Iterator[DenseVector[Double]],
                         filters: DenseMatrix[Double],
                         ngramSize: Int,
                         outSize: Int,
                         alphabetSize: Int = 4): Iterator[DenseVector[Double]] =
  {
    var ngramMat = new DenseMatrix[Double](outSize*alphabetSize, ngramSize)
    seq.map(convolve(_, ngramMat, filters, alphabetSize))
  }
}
