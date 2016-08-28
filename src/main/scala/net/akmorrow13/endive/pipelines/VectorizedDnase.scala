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
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.utils._
import nodes.learning.LogisticRegressionEstimator

import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{ SequenceDictionary, ReferenceRegion }
import org.bdgenomics.adam.models.Coverage
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._
import org.bdgenomics.adam.rdd.ADAMContext._


object VectorizedDnase extends Serializable  {

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
    println("STARTING VECTORIZED PIPELINE")

    // challenge parameters
    val windowSize = 200
    val stride = 50

    // create new sequence with reference path
    val referencePath = conf.reference
    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val dnasePath = conf.dnase

    // load in 1 transcription factor
    val windowsRDD: RDD[LabeledWindow] = Preprocess.loadLabels(sc, labelsPath)
      ._1.map(r => LabeledWindow(Window(r._1, r._2, r._3, "N" * 200), r._4))
      .cache()

    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    /* First one chromesome and one celltype per fold (leave 1 out) */
    val folds = EndiveUtils.generateFoldsRDD(windowsRDD, conf.heldOutCells, conf.heldoutChr, conf.folds)
    val chrCellTypes: Iterable[(String, CellTypes.Value)] = windowsRDD.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys
    val cellTypes = chrCellTypes.map(_._2)


    /************************************
      *  Prepare dnase data
    **********************************/
    val cuts: RDD[Cut] = Preprocess.loadCuts(sc, conf.dnase, cellTypes.toArray)

    val dnase = new Dnase(windowSize, stride, sc, cuts)
    val aggregatedCuts = dnase.merge(sd, true).cache()
    aggregatedCuts.count


    println("TOTAL FOLDS " + folds.size)
    for (i <- (0 until folds.size)) {
      println("FOLD " + i)

      // get testing cell types for this fold
      val cellTypesTest = folds(i)._2.map(x => (x.win.cellType)).countByValue().keys.toList
      println(s"Fold ${i}, testing cell types:")
      cellTypesTest.foreach(println)

      // get testing chrs for this fold
      val chromosomesTest:Iterable[String] = folds(i)._2.map(x => (x.win.getRegion.referenceName)).countByValue().keys
      println(s"Fold ${i}, testing chromsomes:")
      chromosomesTest.foreach(println)

      // calculate features for train and test sets
      val train = featurize(sc, folds(i)._1, aggregatedCuts, sd, true).cache()
      val test = featurize(sc, folds(i)._2, aggregatedCuts, sd, false).cache()

      println("TRAIN SIZE IS " + train.count())
      println("TEST SIZE IS " + test.count())

      // training features
      val xTrainPositives = train.filter(r => r.features.findAll(_ > 0).size > 0).map(_.features)
        .setName("XTrainPositives").cache()
      val xTrainNegatives = train.map(_.features).filter(r => r.findAll(_ > 0).size == 0)
      	.setName("XTrainNegatives").cache()
      println("training positives and negatives", xTrainPositives.count, xTrainNegatives.count)

      // testing features
      val xTestPositives: RDD[DenseVector[Double]] = test.map(_.features).filter(r => r.findAll(_ > 0).size > 0)
	.setName("XTestPositives").cache()
      val xTestNegatives: RDD[DenseVector[Double]] = test.map(_.features).filter(r => r.findAll(_ > 0).size == 0)
	.setName("XTestNegatives").cache()
      println("testing positives and negatives", xTestPositives.count, xTestNegatives.count)
 
      // training labels
      val yTrainPositives = train.filter(r => r.features.findAll(_ > 0).size > 0).map(_.labeledWindow.label)
        .setName("yTrainPositive").cache()
      val yTrainNegatives = train.filter(r => r.features.findAll(_ > 0).size == 0).map(_.labeledWindow.label)
	.setName("yTrainNegative").cache()
      val yTrain = yTrainPositives.union(yTrainNegatives).map(_.toDouble)
      
      // testing labels

      val yTestPositives = test.filter(r => r.features.findAll(_ > 0).size > 0).map(_.labeledWindow.label)
        .setName("yTestPositive").cache()
      val yTestNegatives = test.filter(r => r.features.findAll(_ > 0).size == 0).map(_.labeledWindow.label)
        .setName("yTestNegative").cache()
      val yTest = yTestPositives.union(yTestNegatives).map(_.toDouble)

      println("Training model")
      val predictor = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 10).fit(xTrainPositives, yTrainPositives)

      val yPredTrain = predictor(xTrainPositives).union(xTrainNegatives.map(r => 0.0))
      val evalTrain = new BinaryClassificationMetrics(yPredTrain.zip(yTrain.map(_.toDouble)))
      println("Train Results: \n ")
      Metrics.printMetrics(evalTrain)

      val yPredTest = predictor(xTestPositives).union(xTestNegatives.map(r => 0.0))
      val evalTest = new BinaryClassificationMetrics(yPredTest.zip(yTest.map(_.toDouble)))
      println("Test Results: \n ")
      Metrics.printMetrics(evalTest)

    }
  }


  /**
   * Featurize with full calcuated dnase coverage
   * @param sc SparkContext
   * @param rdd RDD of labeledWindow
   * @param coverage point coverage
   * @param sd SequenceDictionary
   * @param subselectNegatives whether or not to subselect the dataset
   * @return
   */
  def featurize(sc: SparkContext, rdd: RDD[LabeledWindow], coverage: RDD[CutMap], sd: SequenceDictionary, subselectNegatives: Boolean = true): RDD[BaseFeature] = {

    // get cell types in the current dataset
    val chromosomes = Chromosomes.toVector

    // perform optional negative sampling
    val filteredRDD =
      if (subselectNegatives)
        Sampling.subselectSamples(sc, rdd, sd, partition = false)
      else
        rdd

    val labeledGroupedWindows: RDD[(String, Iterable[LabeledWindow])] = filteredRDD
	    .groupBy(w => w.win.getRegion.referenceName)

    val groupedCoverage: RDD[(String, Iterable[CutMap])] = coverage
      .groupBy(r => r.position.referenceName)

    val coverageAndWindows: RDD[(String, (Iterable[LabeledWindow], Option[Iterable[CutMap]]))] = labeledGroupedWindows.leftOuterJoin(groupedCoverage)

    coverageAndWindows.mapValues(iter => {
      val windows: List[LabeledWindow] = iter._1.toList
      val cov: Map[Long, CutMap] = iter._2.getOrElse(Iterator()).map(r => (r.position.pos, r)).toMap

      windows.map(labeledWindow => {

        val cellType = labeledWindow.win.cellType

        val positions: Array[Int] = (labeledWindow.win.region.start until labeledWindow.win.region.end)
          .map(r => {
            val m = cov.get(r)
            if (m.isDefined) m.get.countMap.get(cellType).getOrElse(0)
            else 0
          }).toArray

        // positions should be size of window
        BaseFeature(labeledWindow, DenseVector(positions.map(_.toDouble)))
      }).toIterator
    }).flatMap(_._2)

  }


}