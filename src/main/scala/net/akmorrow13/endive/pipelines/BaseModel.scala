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
import net.akmorrow13.endive.processing._

import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
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

    // create new sequence with reference path
    val referencePath = conf.reference
    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val dnasePath = conf.dnase

    // held out chr and cell type for testing
    val heldoutChr = "chr1"
    val heldoutCellType = "HeLa-S3"

    // RDD of (tf name, celltype, region, score)
    val labels: RDD[(String, String, ReferenceRegion, Double)] = Preprocess.loadLabelFolder(sc, labelsPath)

    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] =
      DatasetCreationPipeline.extractSequencesAndLabels(referencePath, labels)

    // Load DNase data of (cell type, peak record)
    val dnaseRDD: RDD[(String, PeakRecord)] = Preprocess.loadPeakFolder(sc, conf.dnase)

    // merge dnase with sequences
    val dnase = new DNase(windowSize, stride, dnaseRDD)
    val dnaseMapped: RDD[LabeledWindow] = dnase.joinWithSequences(sequences)

    // save data
    dnaseMapped.map(_.toString).saveAsTextFile(conf.aggregatedSequenceLoc)

    // partition into train and test sets
    val dataset = new DataSet(dnaseMapped)
    val train = dataset.train
    val test = dataset.test


    // filter out positives: areas with no dnase are automatic negatives
    val features: RDD[LabeledPoint] = featurize(train)

    //  score using a linear classifier with a log loss function
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(features)


    /*********************************
      * Testing on held out chromosome
    ***********************************/
    val heldOutChr = dataset.heldoutChr
    val chrTest = featurize(test.filter(r => r.win.region.referenceName == heldoutChr))

    var predictionAndLabels = chrTest.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    println("held out chr accuracy")
    Metrics.computeAccuracy(predictionAndLabels)


    /*********************************
      * Testing on held out cell type
      * ***********************************/
    val heldOutCellType = dataset.heldoutCellType
    val cellTypeTest = featurize(test.filter(r => r.win.region.referenceName == heldOutCellType))

    predictionAndLabels = cellTypeTest.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    println("held out cell type accuracy")
    Metrics.computeAccuracy(predictionAndLabels)

  }

  /***********************************
    ** featurize data wth motifs
    *  Bins that do overlap DNASE peaks are scored using a linear classifier with a log loss function
    *  Linear Classifier Input features:
          - Known motifs: -log2(motif score) region summary statistics
          - Max, 0.99%, 0.95%, 0.75%, 0.50%, mean
          - max DNASE fold change across each bin
    *************************************/
  def featurize(rdd: RDD[LabeledWindow]): RDD[LabeledPoint] = {
    rdd.map(r => {
      if (r.win.dnase.length > 0) {
        // known motif score
        val motifScore = Motif.scoreSequence(r.win.tf, r.win.sequence, "any")
        // max DNASE fold change across each bin
        val maxScore = r.win.dnase.map(_.peak).max
        val minScore = r.win.dnase.map(_.peak).min
        val fold = (maxScore - minScore) / minScore
        // TODO: Max, 0.99%, 0.95%, 0.75%, 0.50%, mean
        new LabeledPoint(r.label, Vectors.dense(motifScore, fold))
      } else {
        // no dnase overlap. force to negative example
        new LabeledPoint(0.0, Vectors.dense(0.0,0.0))
      }
    })


  }



}
