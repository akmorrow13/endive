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
import org.apache.spark.{SparkConf, SparkContext}
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
import nodes.learning.LogisticRegressionEstimator
import java.io._

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
      val conf = new SparkConf().setAppName("ENDIVE")
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
    val seed = 0
    val kmerSize = 8
    val approxDim = conf.dim
    val alphabetSize = Dataset.alphabet.size

    val dataPath = conf.aggregatedSequenceOutput
    val dnasePath = conf.dnase
    val referencePath = conf.reference

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:9000"), hadoopConf)

    if (dataPath == null || referencePath == null || dnasePath == null) {
      println("Error: no data or reference path")
      sys.exit(-1)
    }

    // load data for a specific transcription factor
    var allData: RDD[LabeledWindow] =
      LabeledWindowLoader(dataPath, sc).setName("_All data")
  	.repartition(conf.numPartitions)
      .cache()

    if (conf.featurizeSample < 1.0) = {
      allData = allData.sample(false, conf.featurizeSample)
    }

    // either generate filters or load from disk

    val W =
    if (conf.readFiltersFromDisk) {

      val numFilters = scala.io.Source.fromFile(conf.filtersPath).getLines.size
      val dataDimension = scala.io.Source.fromFile(conf.filtersPath).getLines.next.split(",").size

      // Make sure read in filter params are consistent with our world view
      assert(numFilters == approxDim)
      assert(dataDimension ==  kmerSize * alphabetSize)

      val WRaw = scala.io.Source.fromFile(conf.filtersPath).getLines.toArray.flatMap(_.split(",")).map(_.toDouble)
      val W:DenseMatrix[Double] = new DenseMatrix(approxDim, kmerSize * alphabetSize, WRaw)
      W
    } else {
      // Only gaussian filter generation is supported (for now)

      implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
      val gaussian = new Gaussian(0, 1) 

      // generate random matrix
      val W:DenseMatrix[Double] = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian) * conf.gamma
      W
    }

    // generate approximation features
    val allFeaturized = featurize(allData, W, kmerSize).cache()

    allFeaturized.map(_.toString).saveAsTextFile(conf.featuresOutput)
  }

  /**
   * Takes a region of the genome and one hot enodes sequences (eg A = 0001). If DNASE is enabled positive strands
   * are appended to the one hot sequence encoding. For example, if positive 1 has A with DNASE count of 3 positive
   * strands, the encoding is 0003.
   *
   * @param matrix: data matrix with sequences
   * @param W: random matrix
   * @param kmerSize: length of kmers to be created
   */
  def featurize(matrix: RDD[LabeledWindow],
                            W: DenseMatrix[Double],
                            kmerSize: Int): RDD[FeaturizedLabeledWindow] = {

    val kernelApprox = new KernelApproximator(W, Math.cos, ngramSize = kmerSize)

    matrix.map(f => {
      val kx = (kernelApprox({
        KernelApproximator.stringToVector(f.win.sequence)
      }))
      FeaturizedLabeledWindow(f, kx)
    })

  }
}
