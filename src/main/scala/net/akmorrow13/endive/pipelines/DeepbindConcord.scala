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
import com.github.fommil.netlib.BLAS
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import nodes.learning._
import nodes.util.{Cacher, ClassLabelIndicatorsFromIntLabels, MaxClassifier}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import pipelines.Logging


object DeepbindConcord extends EndiveLearningPipeline with Serializable with Logging {

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
    val kmerSize = 8
    val alphabetSize = Dataset.alphabet.size

    val trainPath = "/data/anv/DREAMDATA/deepbind/EGR1_withNegatives/EGR1_GM12878_Egr-1_HudsonAlpha_AC.seq"
    val testPath = "/data/anv/DREAMDATA/deepbind/EGR1_withNegatives/EGR1_GM12878_Egr-1_HudsonAlpha_B.seq"

    // extract tfs and cells for this label file
    val tf = TranscriptionFactors.EGR1
    val cell = CellTypes.GM12878

    val train = sc.textFile(trainPath)
        .filter(!_.contains("FoldID"))
        .map(r => {
          val parts = r.split(" ")
          val w = Window(tf, cell, null, parts(2))
          LabeledWindow(w, parts(3).toInt)
        })

    val test = sc.textFile(testPath)
      .filter(!_.contains("FoldID"))
      .map(r => {
        val parts = r.split("\t")
        val w = Window(tf, cell, null, parts(2))
        LabeledWindow(w, parts(3).toInt)
      })

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val W = 0.1 * DenseMatrix.rand(conf.dim, kmerSize * alphabetSize, gaussian)

    // generate approximation features
    val trainApprox = KernelPipeline.featurize(train, W, kmerSize)
	    .cache()
    val testApprox = KernelPipeline.featurize(test, W, kmerSize)
    	.cache()

    println(trainApprox.count, testApprox.count)

    // threshold labels, required for BlockLeastSquaresEstimator
    val labelExtractor = ClassLabelIndicatorsFromIntLabels(2) andThen
      new Cacher[DenseVector[Double]]
    val trainLabels = labelExtractor(trainApprox.map(_.labeledWindow.label))

    val predictor = new BlockLeastSquaresEstimator(conf.dim, conf.epochs, conf.lambda).fit(trainApprox.map(_.features), trainLabels.get)

    saveModel(conf.modelPath, predictor)

    val trainPredictions = MaxClassifier(predictor(trainApprox.map(_.features)))
    val evalTrain = new BinaryClassificationMetrics(trainPredictions.map(_.toDouble).zip(trainApprox.map(_.labeledWindow.label.toDouble)))
    println("Train Results: \n ")
    Metrics.printMetrics(evalTrain)

    val testPredictions = MaxClassifier(predictor(testApprox.map(_.features)))
    val evalTest = new BinaryClassificationMetrics(testPredictions.map(_.toDouble).zip(testApprox.map(_.labeledWindow.label.toDouble)))
    println("Test Results: \n ")
    Metrics.printMetrics(evalTest)

  }
}
