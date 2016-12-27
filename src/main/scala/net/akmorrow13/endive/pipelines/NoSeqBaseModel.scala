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

import breeze.linalg.DenseVector
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing.{CellTypes, Chromosomes}
import net.akmorrow13.endive.utils._
import nodes.learning.LogisticRegressionEstimator

import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.SequenceDictionary
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object NoSeqBaseModel extends Serializable  {

  /**
   * A very basic dataset creation pipeline that *doesn't* featurize the data
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

    val windowsRDD: RDD[LabeledWindow] = sc.textFile(labelsPath)
      .map(s => LabeledWindowLoader.stringToLabeledWindow(s))
      .cache()

    val records = DatasetCreationPipeline.getSequenceDictionary(referencePath)
                .records.filter(r => Chromosomes.toVector.contains(r.name))

    val sd = new SequenceDictionary(records)

    /* First one chromesome and one celltype per fold (leave 1 out) */
    val folds = EndiveUtils.generateFoldsRDD(windowsRDD.keyBy(r => (r.win.region.referenceName, r.win.cellType)), conf.heldOutCells, conf.heldoutChr, conf.folds)
    val chrCellTypes:Iterable[(String, CellTypes.Value)] = windowsRDD.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys

    println("TOTAL FOLDS " + folds.size)
    for (i <- (0 until folds.size)) {
      println("FOLD " + i)
      val train = featurize(sc, folds(i)._1.map(_._2), sd)
      val test = featurize(sc, folds(i)._2.map(_._2), sd, false)

      println("TRAIN SIZE IS " + train.count())
      println("TEST SIZE IS " + test.count())


      // get testing cell types for this fold
      val cellTypesTest: Iterable[CellTypes.Value] = test.map(x => (x.labeledWindow.win.cellType)).countByValue().keys
      println(s"Fold ${i}, testing cell types:")
      cellTypesTest.foreach(println)

      // get testing chrs for this fold
      val chromosomesTest:Iterable[String] = test.map(x => (x.labeledWindow.win.getRegion.referenceName)).countByValue().keys
      println(s"Fold ${i}, testing chromsomes:")
      chromosomesTest.foreach(println)

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
      val yTestPositives: RDD[Int] = test.filter(_.labeledWindow.win.getDnase.length > 0).map(_.labeledWindow.label).cache()
      val yTestNegatives = test.filter(_.labeledWindow.win.getDnase.length == 0).map(_.labeledWindow.label).cache()
      val yTest = yTestPositives.union(yTestNegatives).map(_.toDouble)

      println("Training model")
      val predictor = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 1).fit(XTrainPositives, yTrainPositives)

      val yPredTrain = predictor(XTrainPositives).union(XTrainNegatives.map(r => 0.0))
      val evalTrain = new BinaryClassificationMetrics(yPredTrain.zip(yTrain))
      println("Train Results: \n ")
      Metrics.printMetrics(evalTrain)

      val yPredTest: RDD[Double] = predictor(XTestPositives).union(XTestNegatives.map(r => 0.0))
      val evalTest = new BinaryClassificationMetrics(yPredTest.zip(yTest))
      println("Test Results: \n ")
      Metrics.printMetrics(evalTest)

    }
  }

  /***********************************
    ** featurize data wth motifs
    *  Bins that do overlap DNASE peaks are scored using a linear classifier with a log loss function
    *  Linear Classifier Input features:
    * - Known motifs: -log2(motif score) region summary statistics
    * - Max, 0.99%, 0.95%, 0.75%, 0.50%, mean
    * - max DNASE fold change across each bin
    *************************************/
  def featurize(sc: SparkContext, rdd: RDD[LabeledWindow], sd: SequenceDictionary, filter: Boolean = true): RDD[FeaturizedLabeledWindow] = {

    val filteredRDD =
      if (filter)
        EndiveUtils.subselectSamples(sc, rdd, sd, partition = false)
      else
        rdd

    filteredRDD
      .map(r => {
        if (r.win.getDnase.length > 0) {
          FeaturizedLabeledWindow(r, DenseVector(1.0))
        } else {
          FeaturizedLabeledWindow(r, DenseVector(0.0))
        }

      })
}


}
