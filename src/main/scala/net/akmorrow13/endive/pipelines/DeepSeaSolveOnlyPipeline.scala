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

import java.io._
import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.{Chromosomes, CellTypes, TranscriptionFactors}
import net.akmorrow13.endive.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.bdgenomics.adam.rdd.feature.CoverageRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{TwoBitFile}
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.BlockLeastSquaresEstimator
import net.jafama.FastMath
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import com.github.fommil.netlib.BLAS

object DeepSeaSolveOnlyPipeline extends Serializable  {

  /**
   * A very basic dataset creation pipeline for sequence data that *doesn't* featurize the data
   * but creates a csv of (Window, Label)
   *
    * @param args
   */
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
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.INFO)
      val conf = new SparkConf().setAppName(configtext)
      conf.setIfMissing("spark.master", "local[4]")
      val sc = new SparkContext(conf)
      val blasVersion = BLAS.getInstance().getClass().getName()
      println(s"Currently used version of blas is ${blasVersion}")
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {
    println("RUN SOLVER ONLY PIPELINE")

    val seqSize = 800
    val kmerSize = conf.kmerLength
    val approxDim = conf.approxDim
    val featuresName = conf.featuresOutput
    val trainFeaturesName = featuresName + "_train.features"
    val valFeaturesName = featuresName + "_val.features"

    // generate headers
    val headers = sc.textFile(conf.deepSeaDataPath + "headers.csv").first().split(",")
    val headerTfs: Array[String] = headers.map(r => r.split('|')).map(r => r(1))
    println(s"headerTfs: ${headerTfs.head} ${headerTfs.length}")

    val trainFiles = sc.textFile(trainFeaturesName)
    val evalFiles = sc.textFile(valFeaturesName)

    val (trainFeatures, evalFeatures, trainLabels, evalLabels) = {
      val trainFeatures: RDD[(Long, DenseVector[Double])] = trainFiles.map(x => DenseVector(x.split("\\|")(0).split(",").map(_.toDouble)))
        .zipWithIndex()
        .map(r => (r._2,r._1))
      val evalFeatures = evalFiles.map(x => DenseVector(x.split("\\|")(0).split(",").map(_.toDouble)))
        .zipWithIndex()
        .map(r => (r._2,r._1))

      val trainLabels: RDD[(Long, LabeledWindow)] = LabeledWindowLoader(s"${conf.getWindowLoc}_train", sc).setName("_All data")
        .zipWithIndex()
        .map(r => (r._2,r._1))
      val evalLabels: RDD[(Long, LabeledWindow)] = LabeledWindowLoader(s"${conf.getWindowLoc}_eval", sc).setName("_eval")
        .zipWithIndex()
        .map(r => (r._2,r._1))

      val train = trainFeatures.join(trainLabels).map(r => r._2)
      val eval = evalFeatures.join(evalLabels).map(r => r._2)

      (train.map(_._1), eval.map(_._1), train.map(r => DenseVector(r._2.labels.map(_.toDouble))),
        eval.map(r => DenseVector(r._2.labels.map(_.toDouble))))

    }

    val model = new BlockLeastSquaresEstimator(min(1024, approxDim), 1, conf.lambda)
      .fit(trainFeatures, trainLabels)
    val allYTrain = model(trainFeatures)
    val allYEval = model(evalFeatures)

    val zippedTrainResults = allYTrain.zip(trainLabels)
    val zippedEvalResults = allYEval.zip(evalLabels)
    zippedEvalResults.map(r => s"${r._1.toArray.mkString(",")}|${r._2.toArray.mkString(",")}")
      .saveAsTextFile(s"${conf.getFeaturizedOutput}_eval")
    zippedTrainResults.map(r => s"${r._1.toArray.mkString(",")}|${r._2.toArray.mkString(",")}")
      .saveAsTextFile(s"${conf.getFeaturizedOutput}_train")

    // get metrics
    val tfs: Array[String] = Array("ATF3","EGR1")
    DnaseKernelPipeline.printAllMetrics(headers, tfs, zippedTrainResults, zippedEvalResults, None)
  }
}
