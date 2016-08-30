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
import net.akmorrow13.endive.utils._
import nodes.learning.LogisticRegressionEstimator

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.SequenceDictionary
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object DnaseModel extends Serializable  {

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

    val windowsRDD: RDD[LabeledWindow] = sc.textFile(labelsPath)
      .map(s => LabeledWindowLoader.stringToLabeledWindow(s))
      .cache()

    val chrCellTypes:Iterable[(String, CellTypes.Value)] = windowsRDD.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys
    val cellTypes = chrCellTypes.map(_._2)

    val records = DatasetCreationPipeline.getSequenceDictionary(referencePath)
      .records.filter(r => Chromosomes.toVector.contains(r.name))

    val sd = new SequenceDictionary(records)

    /************************************
      *  Prepare dnase data
      **********************************/
    val cuts: RDD[Cut] = Preprocess.loadCuts(sc, conf.dnase, cellTypes.toArray)

    val dnase = new Dnase(windowSize, stride, sc, cuts)
    val aggregatedCuts = dnase.merge(sd).cache()
    aggregatedCuts.count

    val featurized = VectorizedDnase.featurize(sc, windowsRDD, aggregatedCuts, sd, None, false)

    /* First one chromesome and one celltype per fold (leave 1 out) */
    val folds = EndiveUtils.generateFoldsRDD(featurized.keyBy(r => (r.labeledWindow.win.region.referenceName, r.labeledWindow.win.cellType)), conf.heldOutCells, conf.heldoutChr, conf.folds)

    println("TOTAL FOLDS " + folds.size)
    for (i <- (0 until folds.size)) {
      println("FOLD " + i)
      val r = new java.util.Random()
      val train = folds(i)._1.map(_._2)
        .filter(x => x.labeledWindow.label == 1 || (x.labeledWindow.label == 0 && r.nextFloat < 0.001))
        .setName("train").cache()

      val test = folds(i)._2.map(_._2)
        .setName("test").cache()


      val xTrain = train.map(_.features)
      val xTest = test.map(_.features)

      val yTrain = train.map(_.labeledWindow.label)
      val yTest = test.map(_.labeledWindow.label)

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

      println("Training model")
      val predictor = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 10, regParam=0.01)
        .fit(xTrain, yTrain)

      val yPredTrain = predictor(xTrain)
      val evalTrain = new BinaryClassificationMetrics(yPredTrain.zip(yTrain.map(_.toDouble)))
      println("Train Results: \n ")
      Metrics.printMetrics(evalTrain)

      val yPredTest = predictor(xTest)
      val evalTest = new BinaryClassificationMetrics(yPredTest.zip(yTest.map(_.toDouble)))
      println("Test Results: \n ")
      Metrics.printMetrics(evalTest)
    }
  }

}
