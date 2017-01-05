/**
 * Copyright 2015 Vaishaal Shankar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.akmorrow13.endive.pipelines

import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.RandomDistribution
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import com.github.fommil.netlib.BLAS
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.{ BlockLeastSquaresEstimator}
import nodes.util.{Cacher, MaxClassifier, ClassLabelIndicatorsFromIntLabels}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, Accumulator}
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import net.jafama.FastMath
import nodes.learning.LogisticRegressionEstimator
import java.io._
import java.util.Random

import java.io.File



object KitchenSinkFeaturizePipeline  extends Serializable with Logging {

  def main(args: Array[String]) = {

    if (args.size < 1) {
      println("Incorrect number of arguments...Exiting now.")
    } else {
      val configfile = scala.io.Source.fromFile(args(0))
      val configtext = try configfile.mkString finally configfile.close()
      println(configtext)
      val yaml = new Yaml(new Constructor(classOf[EndiveConf]))
      val appConfig = yaml.load(configtext).asInstanceOf[EndiveConf]
      EndiveConf.validate(appConfig)
      val conf = new SparkConf().setAppName(appConfig.expName)
      conf.setIfMissing("spark.master", "local[4]")
      Logger.getLogger("org").setLevel(Level.INFO)
      Logger.getLogger("akka").setLevel(Level.INFO)
      val sc = new SparkContext(conf)
      val blasVersion = BLAS.getInstance().getClass().getName()
      println(s"Currently used version of blas is ${blasVersion}")
      run(sc, appConfig)
    }
  }

  def run(sc: SparkContext, conf: EndiveConf): Unit = {
    // set parameters
    val kmerSize = conf.kmerLength
    val approxDim = conf.approxDim
    val alphabetSize = conf.alphabetSize

    val dataPath = conf.aggregatedSequenceOutput
    val referencePath = conf.reference

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)


    val rand = new Random(conf.seed)
    // load data for a specific transcription factor
    var allData: RDD[LabeledWindow] =
      LabeledWindowLoader(dataPath, sc).cache().setName("_All data")
    if (conf.featurizeSample < 1.0)  {

      val samplingIndices = (0 until allData.count().toInt).map(x =>  (x, rand.nextFloat() < conf.featurizeSample)).filter(_._2).map(_._1).toSet
      val samplingIndicesB = sc.broadcast(samplingIndices)

      allData = allData.zipWithIndex.filter(x => samplingIndicesB.value contains x._2.toInt).map(x => x._1).cache()
    }
    allData.count()
    println("NORMALIZING DNASE")
      // normalize dnase
      val dnaseMaxPos = allData.map(r => r.win.dnase.max).max
      allData = allData.map(r => {
        val win = r.win.setDnase(r.win.getDnase / dnaseMaxPos)
        LabeledWindow(win, r.label)
      })
      println(s"max for dnase went from ${dnaseMaxPos} to ${allData.map(r => r.win.dnase.max).max}")
    println("Sampling Frequency " + conf.featurizeSample)

    // either generate filters or load from disk

    val W =
    if (conf.readFiltersFromDisk) {

      val numFilters = scala.io.Source.fromFile(conf.filtersPath).getLines.size
      val dataDimension = scala.io.Source.fromFile(conf.filtersPath).getLines.next.split(",").size

      val WRaw = scala.io.Source.fromFile(conf.filtersPath).getLines.toArray.flatMap(_.split(",")).map(_.toDouble)
      val W:DenseMatrix[Double] = new DenseMatrix(approxDim, kmerSize * alphabetSize + conf.dnaseKmerLength*2, WRaw)
      println(s"W HAS ${W.rows} rows and ${W.cols} cols")
      W
    } else {
      // Only gaussian filter generation is supported (for now)

      implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(conf.seed)))
      val gaussian = new Gaussian(0, 1) 

      // generate random matrix
      val W:DenseMatrix[Double] = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian) * conf.gamma
      W
    }
    val W_seq = W(::, 0 until kmerSize * 4)
    val W_dnase_pos = W(::, kmerSize * 4 until (kmerSize * 4) + conf.dnaseKmerLength)

    // generate approximation features
    val allFeaturized =  DnaseKernelPipeline.featurizeWithDnase(sc, allData, W_seq, conf.kmerLength, Some(W_dnase_pos),  Some(conf.dnaseKmerLength))
    allFeaturized.map(_.toString).saveAsTextFile(conf.featuresOutput)
  }




  def featurizeWithDnaseSeparate(sc: SparkContext,
                         rdd: RDD[LabeledWindow],
                         W: DenseMatrix[Double],
                         kmerSize: Int,
                         dnaseKmerSize: Int,
                         seed: Int = 0,
                         append: Boolean = true) = {

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val numOutputFeatures = W.rows
    val uniform = new Uniform(0, 2*math.Pi)
    val phase0 = DenseVector.rand(numOutputFeatures, uniform)
    val phase1 = DenseVector.rand(numOutputFeatures, uniform)
    val phase2 = DenseVector.rand(numOutputFeatures, uniform)

    val seq_end = kmerSize * 4
    val dnase_pos_end = (kmerSize * 4) + dnaseKmerSize
    val dnase_neg_end = W.cols

    val W_seq = W(::, 0 until kmerSize * 4)
    val W_dnase_pos = W(::, kmerSize * 4 until (kmerSize * 4) + dnaseKmerSize)
    val W_dnase_neg = W(::, ((kmerSize * 4) + dnaseKmerSize until  W.cols))

    val kernelApproxDnase_pos = new KernelApproximator(W_dnase_pos, FastMath.cos, ngramSize = dnaseKmerSize, alphabetSize=1,  offset=Some(phase0))

    val kernelApproxDnase_neg = new KernelApproximator(W_dnase_neg, FastMath.cos, ngramSize = dnaseKmerSize , alphabetSize=1, offset=Some(phase1))

    val kernelApprox_seq = new KernelApproximator(W_seq, FastMath.cos, ngramSize = kmerSize, alphabetSize=4)

    val featurized = rdd.map({ x =>
      val raw_seq = KernelApproximator.stringToVector(x.win.sequence)
      val raw_dnase_pos = x.win.dnase.slice(0, Dataset.windowSize)
      val raw_dnase_neg  = x.win.dnase.slice(Dataset.windowSize, Dataset.windowSize*2)

      val dnase_pos_lift = kernelApproxDnase_pos(raw_dnase_pos)
      val dnase_neg_lift = kernelApproxDnase_neg(raw_dnase_neg)
      val seq_lift = kernelApprox_seq(raw_seq)
      val all_lift =
      if (!append) {
        dnase_pos_lift :* dnase_neg_lift :* seq_lift
      } else {
        DenseVector.vertcat(dnase_pos_lift, dnase_neg_lift, seq_lift)
      }
      FeaturizedLabeledWindow(x, all_lift)
    })
  featurized
}
}
