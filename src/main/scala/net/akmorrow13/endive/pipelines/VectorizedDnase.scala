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
import net.akmorrow13.endive.processing.Dataset
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
    println("STARTING BASEMODEL PIPELINE")

    // challenge parameters
    val windowSize = 200
    val stride = 50


    // create new sequence with reference path
    val referencePath = conf.reference
    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val dnasePath = conf.dnase

    // load in 1 transcription factor
    val windowsRDD: RDD[LabeledWindow] = sc.textFile(labelsPath)
      .map(s => LabeledWindowLoader.stringToLabeledWindow(s))
      .cache()

    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    /* First one chromesome and one celltype per fold (leave 1 out) */
    val folds = EndiveUtils.generateFoldsRDD(windowsRDD, conf.heldOutCells, conf.heldoutChr, conf.folds)
    val chrCellTypes:Iterable[(String, CellTypes.Value)] = windowsRDD.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys

    // load coverage for all cell types
    val coverageFiles = conf.dnase.split(",")

    var coverage: RDD[(CellTypes.Value, ReferenceRegion)] = sc.emptyRDD[(CellTypes.Value, ReferenceRegion)]
    coverageFiles.map(f => {
      val cellType = CellTypes.getEnumeration(f.split("/").last.split('.')(1))

      val alignmentRdd: RDD[(CellTypes.Value, ReferenceRegion)] = sc.loadAlignments(f)
              .rdd
              .filter(!_.getMateNegativeStrand)
              .map(r => (cellType, ReferenceRegion(r)))

      coverage = coverage.union(alignmentRdd)
    })

    println("TOTAL FOLDS " + folds.size)
    for (i <- (0 until folds.size)) {
      println("FOLD " + i)

      // get testing cell types for this fold
      val cellTypesTest = folds(i)._2.map(x => (x.win.cellType)).countByValue().keys.toList
      println(s"Fold ${i}, testing cell types:")
      cellTypesTest.foreach(println)

      val cellTypesTrain = folds(i)._1.map(x => (x.win.cellType)).countByValue().keys.toList

      // get testing chrs for this fold
      val chromosomesTest:Iterable[String] = folds(i)._2.map(x => (x.win.getRegion.referenceName)).countByValue().keys
      println(s"Fold ${i}, testing chromsomes:")
      chromosomesTest.foreach(println)

      val train = featurize(sc, folds(i)._1, coverage, sd, true)
      val test = featurize(sc, folds(i)._2, coverage, sd, false)

      println("TRAIN SIZE IS " + train.count())
      println("TEST SIZE IS " + test.count())

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


  /**
   * Featurize with full calcuated dnase coverage
   * @param sc SparkContext
   * @param rdd RDD of labeledWindow
   * @param coverage point coverage
   * @param sd SequenceDictionary
   * @param subselectNegatives whether or not to subselect the dataset
   * @return
   */
  def featurize(sc: SparkContext, rdd: RDD[LabeledWindow], coverage: RDD[(CellTypes.Value, ReferenceRegion)], sd: SequenceDictionary, subselectNegatives: Boolean = true): RDD[BaseFeature] = {

    val cellTypes = rdd.map(x => (x.win.cellType)).countByValue().keys.toList
    println("cell types: ")
    cellTypes.foreach(println)

    // filter coverage by relevent cellTypes and group by (ReferenceName, CellType)
    val filteredCoverage: RDD[((CellTypes.Value, Chromosomes.Value), Iterator[ReferenceRegion])] =
      coverage
        .filter(r => cellTypes.contains(r._1.toString))
        .groupBy(r => (r._1, Chromosomes.withName(r._2.referenceName)))
        .mapValues(_.map(_._2).toIterator)

    // perform negative sampling
    val filteredRDD =
      if (subselectNegatives)
        Sampling.subselectSamples(sc, rdd, sd, partition = false)
      else
        rdd

    // print sampling statistics
    println(s"filtered rdd ${filteredRDD.count}, original rdd ${rdd.count}")
    println(s"original negative count: ${rdd.filter(_.label == 0.0).count}, " +
      s"negative count after subsampling: ${filteredRDD.filter(_.label == 0.0).count}")

    val labeledGroupedWindows: RDD[((CellTypes.Value, Chromosomes.Value), Iterable[LabeledWindow])] = filteredRDD.groupBy(w => (w.win.getCellType, Chromosomes.withName(w.win.getRegion.referenceName)))
    val coverageAndWindows: RDD[((CellTypes.Value, Chromosomes.Value), (Iterable[LabeledWindow], Option[Iterator[ReferenceRegion]]))] = labeledGroupedWindows.leftOuterJoin(filteredCoverage)

    coverageAndWindows.mapValues(iter => {
      val windows: List[LabeledWindow] = iter._1.toList
      val cov: List[ReferenceRegion] = iter._2.getOrElse(Iterator()).toList

      windows.map(labeledWindow => {
        // filter and flatmap coverage
        val dnase: List[Long] = cov.filter(c => labeledWindow.win.region.overlaps(c))
          .flatMap(r => {
            List(r.start, r.end)
          })

        var positions = (labeledWindow.win.region.start until labeledWindow.win.region.end).map(r => (r, 0.0)).toMap
        // generate map starting at window.start to window.end


        // intersect with coverage
        dnase.foreach(r => {
          positions = positions + (r -> 1.0)
        })
        positions = positions.filterKeys(r => r >= labeledWindow.win.region.start && r <= labeledWindow.win.region.end)
        // positions should be size of window
        assert(positions.size == 200)
        BaseFeature(labeledWindow, DenseVector(positions.values.toArray))
      }).toIterator
    }).flatMap(_._2)

  }


}
