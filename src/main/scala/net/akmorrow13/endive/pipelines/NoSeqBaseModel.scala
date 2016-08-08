/**
 * Copyright 2015 Frank Austin Nothaft
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

import java.io.File
import breeze.linalg.DenseVector
import evaluation.BinaryClassifierEvaluator
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.Motif
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.utils._
import net.akmorrow13.endive.processing.Dataset
import nodes.learning.LogisticRegressionEstimator
import nodes.util.ClassLabelIndicatorsFromIntLabels

import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary, ReferenceRegion}
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object NoSeqBaseModel extends Serializable  {

  /**
   * A very basic dataset creation pipeline that *doesn't* featurize the data
   * but creates a csv of (Window, Label)
   *
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
      conf.setIfMissing("spark.master" ,  "local[4]" )
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {
    println("STARTING BASEMODEL PIPELINE")

    // challenge parameters
    val windowSize = 200
    val stride = 50


    // create new sequence with reference path
    val referencePath = conf.reference
    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val dnasePath = conf.dnase

    val fullMatrix: RDD[LabeledWindow] = sc.textFile(labelsPath)
      .map(s => LabeledWindowLoader.stringToLabeledWindow(s))
      .cache()

    val records = DatasetCreationPipeline.getSequenceDictionary(referencePath)
                .records.filter(r => Dataset.chrs.contains(r.name))

    val sd = new SequenceDictionary(records)

  
    
    val cellTypes: Array[String] = fullMatrix.map(x => (x.win.cellType)).countByValue().keys.toArray
    val folds = cellTypes.size

    val foldsData: RDD[BaseFeature] = featurize(sc, fullMatrix, sd)
      .setName("foldsData")
      .cache()


    val labelVectorizer = ClassLabelIndicatorsFromIntLabels(2)

    for (i <- (0 until folds)) {
      println("calcuated for fold ", i)

      val train = foldsData.filter(x => x.labeledWindow.win.getCellType != cellTypes(i))
        .setName("trainData")
        .cache()
      val test = foldsData.filter(x =>  x.labeledWindow.win.getCellType == cellTypes(i))
        .setName("testData")
        .cache()
      println(train.count, test.count)

      // get training and testing cell types for this fold
      val testCellType = cellTypes(i)
      println(s"Fold ${i}, testing cell types ${testCellType}")

      println(s"Fold ${i}, training points ${train.count()}, testing points ${test.count()}")

      // training features
      val XTrainPositives = train.filter(_.labeledWindow.win.getDnase.length > 0).map(_.features)
        .setName("XTrainPositives").cache()
      val XTrainNegatives = train.filter(_.labeledWindow.win.getDnase.length == 0).map(_.features)

      // testing features
      val XTestPositives: RDD[DenseVector[Double]] = test.filter(_.labeledWindow.win.getDnase.length > 0).map(_.features)
      val XTestNegatives: RDD[DenseVector[Double]] = test.filter(_.labeledWindow.win.getDnase.length == 0).map(_.features)

      // training labels
      val yTrainPositives = train.filter(_.labeledWindow.win.getDnase.length > 0).map(_.labeledWindow.label)
        .setName("yTrainPositive").cache()
      val yTrainNegatives = train.filter(_.labeledWindow.win.getDnase.length == 0).map(_.labeledWindow.label)
        .setName("yTrainNegative").cache()
      val yTrain = yTrainPositives.union(yTrainNegatives).map(_.toDouble)

      // testing labels
      val yTestPositives = test.filter(_.labeledWindow.win.getDnase.length > 0).map(_.labeledWindow.label).cache()
      val yTestNegatives = test.filter(_.labeledWindow.win.getDnase.length == 0).map(_.labeledWindow.label).cache()
      val yTest = yTestPositives.union(yTestNegatives).map(_.toDouble)

      println("Training model")
      val predictor = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 1).fit(XTrainPositives, yTrainPositives)

      val yPredTrain = predictor(XTrainPositives).union(XTrainNegatives.map(r => 0.0))
      val evalTrain = new BinaryClassificationMetrics(yPredTrain.zip(yTrain))
      println("Train Results: \n ")
      Metrics.printMetrics(evalTrain)

      val yPredTest = predictor(XTestPositives).union(XTestNegatives.map(r => 0.0))
      val evalTest = new BinaryClassificationMetrics(yPredTest.zip(yTest))
      println("Test Results: \n ")
      Metrics.printMetrics(evalTest)

    }
  }

  /***********************************
    ** featurize data wth motifs
    *  Bins that do overlap DNASE peaks are scored using a linear classifier with a log loss function
    *  Linear Classifier Input features:
          - Known motifs: -log2(motif score) region summary statistics
          - Max, 0.99%, 0.95%, 0.75%, 0.50%, mean
          - max DNASE fold change across each bin
    *************************************/
  def featurize(sc: SparkContext, rdd: RDD[LabeledWindow], sd: SequenceDictionary): RDD[BaseFeature] = {

    val filteredRDD = Sampling.subselectSamples(sc, rdd, sd, partition = false)
    println(s"filtered rdd ${filteredRDD.count}, original rdd ${rdd.count}")
    println(s"original negative count: ${rdd.filter(_.label == 0.0).count}, " +
      s"negative count after subsampling: ${filteredRDD.filter(_.label == 0.0).count}")

    filteredRDD
      .map(r => {
        if (r.win.getDnase.length > 0) {
          // max DNASE fold change across each bin
          val maxScore = r.win.getDnase.map(_.peak).max
          val minScore = r.win.getDnase.map(_.peak).min
          val dnasefold = (maxScore - minScore) / minScore
          // percentage of overlap
          val x = List.range(r.win.getRegion.start, r.win.getRegion.end)
          val others = r.win.getDnase.flatMap(n => {
            List.range(n.region.start, n.region.end).filter(n => n < r.win.getRegion.start || n > r.win.getRegion.end)
          }).distinct
          val coverage = x.filterNot(r => others.contains(r)).length/r.win.getRegion.length
          BaseFeature(r, DenseVector(coverage, dnasefold))
        } else {
          BaseFeature(r, DenseVector(0.0,0.0))
        }

      })
  }
}
