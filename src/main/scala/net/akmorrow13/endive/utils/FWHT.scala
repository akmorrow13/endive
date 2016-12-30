package utils

import utils.external.NativeRoutines
import breeze.numerics._
import breeze.linalg._

/**
 * Wrapper around FWHT (fast Walsh–Hadamard transform)
 */
object FWHT {

  @transient lazy val extLib = new NativeRoutines()

  def apply(x: DenseVector[Double]): DenseVector[Double] =  {
    assert(isPower2(x.size))
    val arr:Array[Double] = x.data
    new DenseVector(extLib.fwht(arr, x.size))
  }

  def isPower2(num: Int) = {
    num != 0 && ((num & (num - 1)) == 0)
  }

  def nextPower2(num: Int) = {
    pow(2, ceil(log(num)/log(2)));
  }
}

