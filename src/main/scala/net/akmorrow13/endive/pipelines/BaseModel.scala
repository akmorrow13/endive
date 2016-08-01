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
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.Motif
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.utils._
import net.akmorrow13.endive.processing.Dataset
import nodes.util.ClassLabelIndicatorsFromIntLabels

import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}
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


object BaseModel extends Serializable  {

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
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {

    println("STARTING DATA SET CREATION PIPELINE")


    // challenge parameters
    val windowSize = 200
    val stride = 50

    if (conf.featurizedOutput == null)
      throw new Exception("output for featured data not defined")

    val genes = conf.genes
    // create new sequence with reference path
    val referencePath = conf.reference
    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val dnasePath = conf.dnase

    // RDD of (tf name, celltype, region, score)
    val labels: RDD[(String, String, ReferenceRegion, Int)] = Preprocess.loadLabelFolder(sc, labelsPath)

    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] =
      DatasetCreationPipeline.extractSequencesAndLabels(referencePath, labels)

    // Load DNase data of (cell type, peak record)
    val dnase: RDD[(String, PeakRecord)] = Preprocess.loadPeakFolder(sc, dnasePath)
      .cache()

    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    val cellTypeInfo = new CellTypeSpecific(windowSize, stride, dnase, sc.emptyRDD[(String, RNARecord)], sd)


    // deepbind does not have creb1 scores so we will hold out for now
    val fullMatrix: RDD[LabeledWindow] = cellTypeInfo.joinWithDNase(sequences)
      .filter(r => r.win.getTf != "CREB1")

    // filter out positives: areas with no dnase are automatic negatives
    val features: RDD[LabeledPoint] = featurize(sc, fullMatrix, conf.deepbindPath)

//    println("Grouping Data to train and test")
//    val groupedData = fullMatrix.groupBy(lw => lw.win.getRegion.referenceName).cache()
  //  groupedData.count()

    val foldsData = groupedData.map(x => (x._1.hashCode() % conf.folds, x._2))

    val labelVectorizer = ClassLabelIndicatorsFromIntLabels(2)

    for (i <- (0 until conf.folds)) {
      var train = foldsData.filter(x => x._1 != i).flatMap(x => x._2).cache()
      train.count()
      val test = foldsData.filter(x => x._1 == i).flatMap(x => x._2).cache()

      println(s"Fold ${i}, training points ${train.count()}, testing points ${test.count()}")

      val XTrain = train.map(x => denseFeaturize(x.win.getSequence)).setName("XTrain").cache()
      val XTest = test.map(x => denseFeaturize(x.win.getSequence)).cache()
      val yTrain = train.map(_.label).setName("yTrain").cache()
      val yTest = test.map(_.label).cache()

      println("Training model")
      val predictor = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 1).fit(XTrain, yTrain)

      val yPredTrain = predictor(XTrain)
      val evalTrain = BinaryClassifierEvaluator(yTrain.map(_ > 0), yPredTrain.map(_ > 0))
      println("Train Results: \n " +  evalTrain.summary())

      val yPredTest = predictor(XTest)
      val evalTest = BinaryClassifierEvaluator(yTest.map(_ > 0), yPredTest.map(_ > 0))
      println("Test Results: \n " +  evalTest.summary())

    }










    // save features
    features.map(_.toString()).saveAsTextFile(conf.featurizedOutput)
    println(features.first)

    //  score using a linear classifier with a log loss function
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(features)

    val trainingPrediction = features.map { case LabeledPoint(label, features) =>
            val prediction = model.predict(features)
            (prediction, label)
      }

      println("training accuracy")
      Metrics.computeAccuracy(trainingPrediction)
//

    //    /*********************************
//      * Testing on held out chromosome
//    ***********************************/
//    val heldoutChr = dataset.heldoutChr
//    val chrTest = featurize(sc, test.filter(r => r.win.region.referenceName == heldoutChr), conf.deepbindPath)
//
//    var predictionAndLabels = chrTest.map { case LabeledPoint(label, features) =>
//      val prediction = model.predict(features)
//      (prediction, label)
//    }
//
//    println("held out chr accuracy")
//    Metrics.computeAccuracy(predictionAndLabels)
//
//
//    /*********************************
//      * Testing on held out cell type
//      * ***********************************/
//    val heldOutCellType = dataset.heldoutCellType
//    val cellTypeTest = featurize(sc, test.filter(r => r.win.region.referenceName == heldOutCellType), conf.deepbindPath)
//
//    predictionAndLabels = cellTypeTest.map { case LabeledPoint(label, features) =>
//      val prediction = model.predict(features)
//      (prediction, label)
//    }
//    println("held out cell type accuracy")
//    Metrics.computeAccuracy(predictionAndLabels)

  }

  /***********************************
    ** featurize data wth motifs
    *  Bins that do overlap DNASE peaks are scored using a linear classifier with a log loss function
    *  Linear Classifier Input features:
          - Known motifs: -log2(motif score) region summary statistics
          - Max, 0.99%, 0.95%, 0.75%, 0.50%, mean
          - max DNASE fold change across each bin
    *************************************/
  def featurize(sc: SparkContext, rdd: RDD[LabeledWindow], deepbindPath: String): RDD[LabeledPoint] = {
    val motif = new Motif(sc, deepbindPath)
    val filteredPositives = rdd.filter(_.win.getDnase.length > 0)
    val motifScores: RDD[(Map[String, Double], LabeledWindow)] =
      motif.scoreSequences(Dataset.tfs, filteredPositives.map(_.win.getSequence)).zip(filteredPositives)

    motifScores
        .map(r => {
          // known motif score
          // max DNASE fold change across each bin
          val maxScore = r._2.win.getDnase.map(_.peak).max
          val minScore = r._2.win.getDnase.map(_.peak).min
          val fold = (maxScore - minScore) / minScore
          new LabeledPoint(r._2.label, Vectors.dense(r._1(r._2.win.getTf), fold))
        })
  }
}
