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

import java.util.Random

import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.RandomDistribution
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import com.github.fommil.netlib.BLAS
import net.jafama.FastMath
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.{PerClassWeightedLeastSquaresEstimator, BlockLeastSquaresEstimator}
import nodes.util.{Cacher, MaxClassifier, ClassLabelIndicatorsFromIntLabels}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import java.io.{PrintWriter, File}



object SolveAndTestPipeline extends Serializable with Logging {

  /**
   * A very basic pipeline that *doesn't* featurize the data
   * simple regresses the raw sequence with the labels for the sequence
   *
   * HUGE CAVEATS
   * Trains a separate model for each TF type
   * Ignores cell type information
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
    val approxDim = conf.approxDim
    val dnaseSize = 100
    val alphabetSize = Dataset.alphabet.size

    val trainPath = conf.aggregatedSequenceOutput.split(',')(0)
    val testPath = conf.aggregatedSequenceOutput.split(',')(1)
    val dnasePath = conf.dnaseNarrow
    val referencePath = conf.reference

    var featuresOutput = conf.featuresOutput

    if (trainPath == null || testPath == null) {
      println("Error: no train and test paths")
      sys.exit(-1)
    }

    var (trainRDD, testRDD) = (FeaturizedLabeledWindowLoader(trainPath, sc),
      FeaturizedLabeledWindowLoader(testPath, sc))

    if (conf.negativeSamplingFreq < 1.0) {
      println("NEGATIVE SAMPLING")
      val rand = new Random(conf.seed)
      val negativesFull = trainRDD.filter(_.labeledWindow.label == 0)
      val samplingIndices = (0 until negativesFull.count().toInt).map(x => (x, rand.nextFloat() < conf.negativeSamplingFreq)).filter(_._2).map(_._1).toSet
      val samplingIndicesB = sc.broadcast(samplingIndices)
      val negatives = negativesFull.zipWithIndex.filter(x => samplingIndicesB.value contains x._2.toInt).map(x => x._1)

      val positives = trainRDD.filter(_.labeledWindow.label > 0)
      trainRDD = positives.union(negatives)
    }

    val labelExtractor = ClassLabelIndicatorsFromIntLabels(2) andThen
      new Cacher[DenseVector[Double]]

    val trainFeatures = trainRDD.map(_.features)
    val trainLabels = labelExtractor(trainRDD.map(_.labeledWindow.label)).get

    // Currently hardcoded to just do exact solve (after accumulating XtX)
    val model =
      if (conf.mixtureWeight > 0) {
        new PerClassWeightedLeastSquaresEstimator(conf.approxDim, 1, conf.lambda, conf.mixtureWeight).fit(trainFeatures, trainLabels)
      } else {
        new BlockLeastSquaresEstimator(conf.approxDim, 1, conf.lambda).fit(trainFeatures, trainLabels)
      }

    val testFeatures = testRDD.map(_.features)

    var testPredictions: RDD[Double] = model(testFeatures).map(x => x(1))
    val (min, max) = (testPredictions.min(), testPredictions.max())
    testPredictions = testPredictions.map(r => (r - min) / (max - min))
    testPredictions.count()

    val testPredictionsOutput = conf.predictionsOutput
    val array = testRDD.zip(testPredictions).sortBy(_._1.labeledWindow.win.getRegion)
      .map(xy => (xy._1.labeledWindow.win.getRegion, xy._2))
      .collect

    val sorted: Array[String] = (
      if (conf.ladderBoard) {
        (array.filter(_._1.referenceName == "chr1") ++ array.filter(_._1.referenceName == "chr21") ++ array.filter(_._1.referenceName == "chr8"))
      } else if (conf.testBoard) {
        // chronological ordering
        array
      } else {
        throw new Exception("either ladderBoard or testBoard must be specified")
      }).map(r => s"${r._1.referenceName}\t${r._1.start}\t${r._1.end}\t${r._2}\n")

    val writer = new PrintWriter(new File(testPredictionsOutput))
    sorted.foreach(r => writer.write(r))
    writer.close()

  }
}
