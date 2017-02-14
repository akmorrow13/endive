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
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.metrics.Metrics
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.{LassoWithSGD, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import nodes.learning.BlockLeastSquaresEstimator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import com.github.fommil.netlib.BLAS
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object LogisticPipeline extends Serializable  {

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
    val trainFeaturesName = featuresName + s"_train.features"
    val valFeaturesName = featuresName + s"_val.features"

    // generate headers
    val headers = sc.textFile(conf.deepSeaDataPath + "headers.csv").first().split(",")
    val headerTfs: Array[String] = headers.map(r => r.split('|')).map(r => r(1))
    println(s"headerTfs: ${headerTfs.head} ${headerTfs.length}")

    val indexTf = headers.zipWithIndex.filter(r => r._1.contains(conf.tfs)).head
    println(indexTf)


    val trainFiles = sc.textFile(trainFeaturesName)
    val evalFiles = sc.textFile(valFeaturesName)

    val (trainFeatures, evalFeatures, trainLabels, evalLabels) = {
      val trainFeatures = trainFiles.map(x => DenseVector(x.split(",").map(_.toDouble))).zipWithIndex
      val trainLabels = sc.textFile(conf.labels + "_train").map(x => DenseVector(x.split(",").drop(1).map(_.toDouble))).zipWithIndex

      val evalFeatures = evalFiles.map(x => DenseVector(x.split(",").map(_.toDouble))).zipWithIndex
      val evalLabels = sc.textFile(conf.labels + "_eval").map(x => DenseVector(x.split(",").drop(1).map(_.toDouble))).zipWithIndex

     val train: RDD[(DenseVector[Double], DenseVector[Double])] = trainFeatures.map(r => (r._2, r._1)).join(trainLabels.map(r => (r._2, r._1))).map(_._2)
     val eval: RDD[(DenseVector[Double], DenseVector[Double])] = evalFeatures.map(r => (r._2, r._1)).join(evalLabels.map(r => (r._2, r._1))).map(_._2)

      (train.map(_._1), eval.map(_._1), train.map(_._2), eval.map(_._2))
    }

    trainFeatures.cache()
    evalFeatures.cache()
    trainLabels.cache()
    evalLabels.cache()

    println(s"train features: ${trainFeatures.count}, eval features:${evalFeatures.count}, " +
      s"train labels:  ${trainLabels.count}, eval labels: ${evalLabels.count}")

    // Run training algorithm to build the model
    val training: RDD[LabeledPoint] = trainFeatures.zip(trainLabels).map(r => LabeledPoint(r._2(indexTf._2), Vectors.dense(r._1.toArray)))
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

    // Compute raw scores on the test set.
    val trainPredLog: RDD[(Double, Double)] = trainFeatures.zip(trainLabels).map(r => {
      val prediction = model.predict(Vectors.dense(r._1.toArray))
      (prediction, r._2(indexTf._2))
    })

    trainPredLog.map(r => s"${r._1},${r._2}").saveAsTextFile(s"${conf.getFeaturizedOutput}__logistic_train")

    // Get train metrics.
    var metricsTrain = new BinaryClassificationMetrics(trainPredLog)
    println(s"Train ROC: ${metricsTrain.areaUnderROC()}, auPRC:${metricsTrain.areaUnderPR()}")

    // Compute raw scores on the test set.
    val evalPredLog: RDD[(Double, Double)] = evalFeatures.zip(evalLabels).map(r => {
      val prediction = model.predict(Vectors.dense(r._1.toArray))
      (prediction, r._2(indexTf._2))
    })
    evalPredLog.map(r => s"${r._1},${r._2}").saveAsTextFile(s"${conf.getFeaturizedOutput}__logistic_eval")

    // Get eval metrics.
    var metricsEval = new BinaryClassificationMetrics(evalPredLog)
    println(s"Train ROC: ${metricsEval.areaUnderROC()}, auPRC:${metricsEval.areaUnderPR()}")

    /****************** End Logistic ***************************************************/


    /****************** Start Lasso  ***************************************************/
    val lassoModel = new LassoWithSGD()
      .run(training)

    // Compute raw scores on the test set.
    val trainPredLasso: RDD[(Double, Double)] = trainFeatures.zip(trainLabels).map(r => {
      val prediction = lassoModel.predict(Vectors.dense(r._1.toArray))
      (prediction, r._2(indexTf._2))
    })

    trainPredLasso.map(r => s"${r._1},${r._2}").saveAsTextFile(s"${conf.getFeaturizedOutput}__lasso_train")

    // Get train metrics.
    metricsTrain = new BinaryClassificationMetrics(trainPredLasso)
    println(s"Train ROC: ${metricsTrain.areaUnderROC()}, auPRC:${metricsTrain.areaUnderPR()}")

    // Compute raw scores on the test set.
    val evalPredLasso: RDD[(Double, Double)] = evalFeatures.zip(evalLabels).map(r => {
      val prediction = model.predict(Vectors.dense(r._1.toArray))
      (prediction, r._2(indexTf._2))
    })
    evalPredLasso.map(r => s"${r._1},${r._2}").saveAsTextFile(s"${conf.getFeaturizedOutput}__lasso_eval")

    // Get eval metrics.
    metricsEval = new BinaryClassificationMetrics(evalPredLog)
    println(s"Train ROC: ${metricsEval.areaUnderROC()}, auPRC:${metricsEval.areaUnderPR()}")

    /**************************End Lasso ***********************************************************/


    val blockModel = new BlockLeastSquaresEstimator(min(1024, approxDim), 1, conf.lambda)
      .fit(trainFeatures, trainLabels)
    val allYTrain = blockModel(trainFeatures)
    val allYEval = blockModel(evalFeatures)
    val zippedEvalResults = allYEval.zip(evalLabels)

    val evalEval = new BinaryClassificationMetrics(zippedEvalResults.map(r => (r._1(indexTf._2), r._2(indexTf._2))))
    Metrics.printMetrics(evalEval, Some(s"Eval,${indexTf._1},${indexTf._2}"))

  }
}
