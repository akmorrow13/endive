package utils.external

class NativeRoutines extends Serializable {
  System.loadLibrary("NativeRoutines") // This will load libNativeRoutines.{so,dylib} from the library path.

  /**
   * Sum-Pools and Symmetric rectifies an image
   * Input image must be in column major vectorized format. (See image.scala in
   * keystone-ml for more details)
   */

  @native
  def poolAndRectify(stride: Int, poolSize: Int,
    numChannels: Int = 3, xDim: Int, yDim: Int,
    maxVal: Double = 0.0, alpha: Double = 0.0, image: Array[Double]): Array[Double]

  @native
  def fwht(in: Array[Double], length: Int) : Array[Double]

  @native
  def fastfood(gaussian: Array[Double],
               radamacher: Array[Double],
               chiSquared: Array[Double],
               patchMatrix: Array[Double],
               seed: Int,
               outSize: Int,
               inSize: Int,
               numPatches: Int) : Array[Double]

  @native
  def cosine(in: Float) : Float

  @native
  def NewDualLeastSquaresEstimator(n: Int, k: Int, d: Int, lambda: Double): Long

  @native
  def DeleteDualLeastSquaresEstimator(ptr: Long): Unit

  @native
  def DualLeastSquaresEstimatorAccumulateGram(ptr: Long, data: Array[Double]): Unit

  @native
  def DualLeastSquaresEstimatorSolve(ptr: Long, labels: Array[Double]): Array[Double]
}
