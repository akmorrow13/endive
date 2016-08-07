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

    if (conf.featurizedOutput == null)
      throw new Exception("output for featured data not defined")

    val genes = conf.genes
    // create new sequence with reference path
    val referencePath = conf.reference
    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val dnasePath = conf.dnase

    // RDD of (tf name, celltype, region, score)
    val fullMatrix: RDD[LabeledWindow] = sc.textFile(labelsPath)
      .map(s => LabeledWindowLoader.stringToLabeledWindow(s))

    val tfs = fullMatrix.map(_.win.tf).distinct.collect()
    println("running on tfs:")
    tfs.foreach(println)

    // extract sequences from reference over training regions
//    val sequences: RDD[LabeledWindow] =
//      DatasetCreationPipeline.extractSequencesAndLabels(referencePath, labels)

    // Load DNase data of (cell type, peak record)
//    val dnase: RDD[(String, PeakRecord)] = Preprocess.loadPeakFolder(sc, dnasePath)
//
    val records = DatasetCreationPipeline.getSequenceDictionary(referencePath)
      .records.filter(r => Dataset.chrs.contains(r.name))

    val sd = new SequenceDictionary(records)

//    val cellTypeInfo = new CellTypeSpecific(windowSize, stride, dnase, sc.emptyRDD[(String, RNARecord)], sd)


    // deepbind does not have creb1 scores so we will hold out for now
//    val fullMatrix: RDD[LabeledWindow] = cellTypeInfo.joinWithDNase(sequences)
//      .filter(r => r.win.getTf != "CREB1")

    val foldsData: RDD[(BaseFeature, Int)] = featurize(sc, fullMatrix,tfs, conf.motifDBPath, None, sd)
          .map(r => (r, r.labeledWindow.win.getRegion.referenceName.hashCode % conf.folds))
          .setName("foldsData")
          .cache()

    println(s"joined with motifs ${foldsData.count}")

    // save featurized data
    foldsData.map(r => r._1.labeledWindow.toString()).saveAsTextFile(conf.featurizedOutput)

    val labelVectorizer = ClassLabelIndicatorsFromIntLabels(2)

    for (i <- (0 until conf.folds)) {
      println("calcuated for fold ", i)
      
      val train = foldsData.filter(x => x._2 != i).map(x => x._1)
        .setName("trainData")
        .cache()
      train.count()
      val test = foldsData.filter(x => x._2 == i).map(x => x._1)
        .setName("testData")
        .cache()

      println(s"Fold ${i}, training points ${train.count()}, testing points ${test.count()}")

      // training features
      val XTrainPositives = train.filter(_.labeledWindow.win.getDnase.length > 0).map(_.features)
        .setName("XTrainPositives").cache()
      val XTrainNegatives = train.filter(_.labeledWindow.win.getDnase.length == 0).map(_.features)

      // testing features
      val XTestPositives = test.filter(_.labeledWindow.win.getDnase.length > 0).map(_.features)
      val XTestNegatives = test.filter(_.labeledWindow.win.getDnase.length == 0).map(_.features)

      // training labels
      val yTrainPositives = train.filter(_.labeledWindow.win.getDnase.length > 0).map(_.labeledWindow.label)
        .setName("yTrainPositive").cache()
      val yTrainNegatives = train.filter(_.labeledWindow.win.getDnase.length == 0).map(_.labeledWindow.label)
        .setName("yTrainNegative").cache()

      // testing labels
      val yTestPositives = test.filter(_.labeledWindow.win.getDnase.length > 0).map(_.labeledWindow.label).cache()
      val yTestNegatives = test.filter(_.labeledWindow.win.getDnase.length == 0).map(_.labeledWindow.label).cache()

      println("Training model")
      val predictor = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 1).fit(XTrainPositives, yTrainPositives)

      val yPredTrain = predictor(XTrainPositives).union(XTrainNegatives.map(r => 0.0))
      val evalTrain = BinaryClassifierEvaluator(yTrainPositives.union(yTrainNegatives).map(_ > 0), yPredTrain.map(_ > 0))
      println("Train Results: \n " +  evalTrain.summary())

      val yPredTest = predictor(XTestPositives).union(XTestNegatives.map(r => 0.0))
      val evalTest = BinaryClassifierEvaluator(yTestPositives.map(_ > 0)union(yTestNegatives).map(_ > 0), yPredTest.map(_ > 0))
      println("Test Results: \n " +  evalTest.summary())

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
  def featurize(sc: SparkContext, rdd: RDD[LabeledWindow],
                tfs: Array[String],
                motifDB: String,
                deepbindPath: Option[String] = None,
                sd:SequenceDictionary): RDD[BaseFeature] = {
    val motif = new Motif(sc, sd)

    val filteredPositives = rdd.filter(_.win.getDnase.length > 0)
    val filteredNegatives = rdd.filter(_.win.getDnase.length == 0)
          .map(r => {
            BaseFeature(r, DenseVector(0.0,0.0,0.0,0.0))
          })

    val motifScores: RDD[LabeledWindow] =
        motif.scoreMotifs(filteredPositives, 200, 50, tfs, motifDB)

    println("records with overlapping dnase count: ", filteredPositives.count)
    println("records without overlapping dnase count: ", filteredNegatives.count)

    val positives = motifScores
        .map(r => {
          // known motif score
          // max DNASE fold change across each bin
          val tf = r.win.getTf
          val maxScore = r.win.getDnase.map(_.peak).max
          val minScore = r.win.getDnase.map(_.peak).min
          val dnasefold = (maxScore - minScore) / minScore
          val motifs = r.win.motifs
          val max: Double = motifs.map(_.peak).max
          val min: Double = motifs.map(_.peak).min
          val mean =
            if (motifs.length > 0) 0.0
             else r.win.motifs.map(_.peak).sum/motifs.length

          BaseFeature(r, DenseVector(min, max, mean, dnasefold))
        })

    positives.union(filteredNegatives)
  }
}

case class BaseFeature(labeledWindow: LabeledWindow, features: DenseVector[Double]) {

  override def toString: String = {
    labeledWindow.toString + "!" + features.toString
  }
}
