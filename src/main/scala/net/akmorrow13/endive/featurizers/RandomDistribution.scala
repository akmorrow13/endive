package net.akmorrow13.endive.featurizers

import breeze.linalg.DenseVector
import breeze.stats.distributions.{ExponentialFamily, DiscreteDistr, Poisson, Gaussian}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object RandomDistribution {

  def poisson(data: RDD[DenseVector[Double]]): Poisson = {
    val summary: MultivariateStatisticalSummary = Statistics.colStats(data.map(r => Vectors.dense(r.toArray)))
    val mean = summary.mean.toArray.sum
    new Poisson(mean)
  }

  def gaussian(data: RDD[DenseVector[Double]]): Gaussian = {
      val summary: MultivariateStatisticalSummary = Statistics.colStats(data.map(r => Vectors.dense(r.toArray)))
      val mean = summary.mean.toArray.sum
      val variance = summary.variance.toArray.sum
      println(s"mean: ${mean} variance ${variance}")
    new Gaussian(mean, variance)
  }
}