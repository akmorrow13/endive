package nodes.stats

import breeze.linalg._
import breeze.numerics._
import breeze.numerics.cos
import breeze.stats.distributions.{Rand, ChiSquared, Bernoulli}
import breeze.stats.mean
import org.apache.spark.rdd.RDD
import utils.{MatrixUtils, FWHT}
import workflow.Transformer
import breeze.stats.distributions._
import org.apache.commons.math3.random.MersenneTwister
import utils.external.NativeRoutines




class FastfoodBatch(
  val g: DenseVector[Double], // should be out long
  val out: Int, // Num output features
  val seed: Int = 11, // rng seed
  val sigma: Double = 1.0 // rng seed
  ) // should be numOutputFeatures by 1
  extends Transformer[DenseMatrix[Double], DenseMatrix[Double]] {

  assert(g.size == out)
  assert(FWHT.isPower2(out))
  @transient lazy val extLib = new NativeRoutines()

  implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
  var B = DenseVector.rand(out, new Bernoulli(0.5, randBasis)).map(if (_) -1.0 else 1.0)
  val P:IndexedSeq[Int] = randBasis.permutation(out).draw()
  val S = (DenseVector.rand(out, ChiSquared(out)) :^ 0.5) :*  1.0/norm(g)
  println("CREATING FASTFOOD NODE")

  override def apply(inRaw: DenseMatrix[Double]): DenseMatrix[Double] =  {

    val in =
    if (!FWHT.isPower2(inRaw.cols)) {
      val inPad = DenseMatrix.zeros[Double](inRaw.rows, FWHT.nextPower2(inRaw.cols).toInt)
      inPad(::, 0 until inRaw.cols) := inRaw
      inPad
    } else {
      inRaw
    }

    assert(FWHT.isPower2(in.cols))
    /*  Since we need to do FWHT over each patch we should first convert the data such that each patch is contigious in memory (a column since breeze is column major */
    val patchMatrixCols = in.t.toArray
    val outArray = extLib.fastfood(g.data, B.data, S.data, patchMatrixCols, seed, out, in.cols, in.rows)
    val dm = new DenseMatrix(out, in.rows, outArray)
    val dmt = new DenseMatrix(in.rows, out, dm.t.toArray)
    val scale = 1.0/(sigma*sqrt(in.cols))
    val batchOut = scale * dmt
    batchOut
  }

  def applyVector(in: DenseVector[Double]): DenseVector[Double] =  {
    val d = FWHT.nextPower2(in.size).toInt
    val inPad = padRight(in, d, 0.0)
    val blocks =
      for (i <- List.range(0, out/d)) yield processBlock(inPad, g(i*d until (i+1)*d), B(i*d until (i+1)*d), P.slice(i*d,(i+1)*d).map(_ % d), S(i*d until (i+1)*d))
    var outVector = DenseVector.vertcat(blocks:_*)
    outVector
  }

  def processBlock(in: DenseVector[Double], G: DenseVector[Double], B: DenseVector[Double], P: IndexedSeq[Int], S: DenseVector[Double]): DenseVector[Double] = {
    val d = in.size
    var W:DenseVector[Double] = FWHT(B :* in)
    1/(sigma*sqrt(d)) :* S :* FWHT(G :* W)
  }
}

