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

object SolverPipeline extends Serializable with Logging {

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

  def trainTestSplit(featuresRDD: RDD[FeaturizedLabeledWindow], testChromosomes: Array[String], testCellTypes: Array[Int]): (RDD[FeaturizedLabeledWindow], RDD[FeaturizedLabeledWindow])  = {
      // hold out a (chromosome, celltype) pair for test
      val trainFeaturizedWindows = featuresRDD.filter { r => 
                  val chrm:String = r.labeledWindow.win.getRegion.referenceName
                  val label:Int = r.labeledWindow.label
                  val cellType:Int = r.labeledWindow.win.cellType.id
                  !testChromosomes.contains(chrm)  && !testCellTypes.contains(cellType) && label != - 1
      }

      val testFeaturizedWindows = featuresRDD.filter { r => 
                  val chrm:String = r.labeledWindow.win.getRegion.referenceName
                  val label:Int = r.labeledWindow.label
                  val cellType:Int = r.labeledWindow.win.cellType.id
                  testChromosomes.contains(chrm)  && testCellTypes.contains(cellType)
      }

      (trainFeaturizedWindows, testFeaturizedWindows)
  }


  def run(sc: SparkContext, conf: EndiveConf): Unit = {

    println(conf.featuresOutput)
    println(sc.textFile(conf.featuresOutput).first.toString)
    val featuresRDD = FeaturizedLabeledWindowLoader(conf.featuresOutput, sc)
    featuresRDD.count()
    println(featuresRDD.first.toString)

    val (trainFeaturizedWindows, testFeaturizedWindows)  = trainTestSplit(featuresRDD, conf.testChromosomes, conf.testCellTypes)
    val labelExtractor = ClassLabelIndicatorsFromIntLabels(2) andThen
      new Cacher[DenseVector[Double]]

    val trainFeatures = trainFeaturizedWindows.map(_.features)
    val trainLabels = labelExtractor(trainFeaturizedWindows.map(_.labeledWindow.label)).get

    val testFeatures = testFeaturizedWindows.map(_.features)
    val testLabels = labelExtractor(testFeaturizedWindows.map(_.labeledWindow.label)).get

    // Currently hardcoded to just do exact solve (after accumulating XtX)
    val model = new BlockLeastSquaresEstimator(conf.approxDim, 1, conf.lambda).fit(trainFeatures, trainLabels)

    val trainPredictions:RDD[Double] = model(trainFeatures).map(x => x(1))
    val testPredictions:RDD[Double] = model(testFeatures).map(x => x(1))

    trainPredictions.count()
    testPredictions.count()

    val trainScalarLabels = trainLabels.map(x => if(x(1) == 1) 1 else 0)
    val testScalarLabels = testLabels.map(x => if(x(1) ==1) 1 else 0)

    val trainPredictionsOutput = conf.predictionsOutput + "/trainPreds"
    val testPredictionsOutput = conf.predictionsOutput + "/testPreds"

    println("WRITING TRAIN PREDICTIONS TO DISK")
    val zippedTrainPreds = trainScalarLabels.zip(trainPredictions).map(x => s"${x._1},${x._2}").saveAsTextFile(trainPredictionsOutput)

    println("WRITING TEST PREDICTIONS TO DISK")
    val zippedTestPreds = testScalarLabels.zip(testPredictions).map(x => s"${x._1},${x._2}").saveAsTextFile(testPredictionsOutput)
  }
}

