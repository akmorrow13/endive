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
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.Sequence
import net.akmorrow13.endive.utils._
import net.akmorrow13.endive.processing.{Dataset, TranscriptionFactors, CellTypes}

import nodes.learning._
import nodes.stats._
import nodes.util._
import nodes.nlp._

import utils.{Image, MatrixUtils, Stats, ImageMetadata, LabeledImage, RowMajorArrayVectorizedImage, ChannelMajorArrayVectorizedImage}
import workflow.{Pipeline, Transformer}
import com.github.fommil.netlib.BLAS
import evaluation.BinaryClassifierEvaluator
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import scala.util.Random

object GlassmanPipeline  extends Serializable with Logging {

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
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
      val sc = new SparkContext(conf)
      val blasVersion = BLAS.getInstance().getClass().getName()
      println(s"Currently used version of blas is ${blasVersion}")
      run(sc, appConfig)
    }
  }

  def denseFeaturize(in: String): DenseVector[Double] = {
    /* Identity featurizer */

   val BASEPAIRMAP = Map('N'-> -1, 'A' -> 0, 'T' -> 1, 'C' -> 2, 'G' -> 3)
    val sequenceVectorizer = ClassLabelIndicatorsFromIntLabels(4)

    val intString:Seq[Int] = in.map(BASEPAIRMAP(_))
    val seqString = intString.map { bp =>
      if (bp == -1) {
        DenseVector.zeros[Double](4)
      } else {
        sequenceVectorizer(bp)
      }
    }
    DenseVector.vertcat(seqString:_*)
  }




  def run(sc: SparkContext, conf: EndiveConf) {
    val dataTxtRDD:RDD[String] = sc.textFile(conf.aggregatedSequenceOutput, minPartitions=600)

    val allData:RDD[LabeledWindow] = LabeledWindowLoader(conf.aggregatedSequenceOutput, sc).setName("All Data").cache()
    allData.count()

    val labelVectorizer = ClassLabelIndicatorsFromIntLabels(2)
    val foldsData = EndiveUtils.generateFoldsRDD(allData.keyBy(r => (r.win.region.referenceName, r.win.cellType)), numFolds = 2)
      /* Make an estimator? */
    val cellTypeFeaturizer = Transformer.apply[LabeledWindow, Int](x => x.win.cellType.id) andThen new ClassLabelIndicatorsFromIntLabels(CellTypes.toVector.size)


    for (i <- (0 until conf.folds)) {

      println("Fold " + i)
      println("HELD OUT CELL TYPES " + foldsData(i)._3.mkString(","))
      println("HELD OUT CHROMOSOMES " + foldsData(i)._4.mkString(","))
      val r = new java.util.Random()
      var train = foldsData(i)._1.map(_._2).filter(x => x.label == 1 || (x.label == 0 && r.nextFloat < 0.001)).setName("train").cache()
      train.count()
      val test = foldsData(i)._2.map(_._2).setName("test").cache()
      test.count()

      val yTrain = train.map(_.label).setName("yTrain").cache()
      val yTest = test.map(_.label).setName("yTest").cache()
      val NUM_FEATURES = 4096

      println(s"Fold ${i}, training points ${train.count()}, testing points ${test.count()}")

      println("Building Pipeline")
      val sequenceFeaturizer =
      Transformer.apply[LabeledWindow, String](x => x.win.sequence) andThen
      Trim andThen
      LowerCase() andThen
      Tokenizer("|") andThen
      NGramsFeaturizer(4 to 7) andThen
      TermFrequency(x => 1.0) andThen
      (CommonSparseFeatures[Seq[String]](NUM_FEATURES), train) andThen
      Transformer.apply[SparseVector[Double], DenseVector[Double]](x => x.toDenseVector)
      val featurizer = Pipeline.gather[LabeledWindow, DenseVector[Double]] {
        sequenceFeaturizer :: (cellTypeFeaturizer) :: Nil
       } andThen VectorCombiner() andThen new Cacher

     val labelGrabber = ClassLabelIndicatorsFromIntLabels(2) andThen new Cacher[DenseVector[Double]]

      val trainFeaturized = featurizer(train).get()
      val testFeaturized = featurizer(test).get()

      val values = testFeaturized.mapPartitions(rows => {
          if (!rows.isEmpty) {
            val rowsArr = rows.toArray
            val nRows = rowsArr.length
            val nCols = rowsArr(0).length
            Iterator.single((nRows, nCols))
          } else {
            Iterator.empty
          }
      }).countByValue()

      println("COUNT " + values.mkString(","))



      /*
     val model = new BlockWeightedLeastSquaresEstimator(4096, 1, 0.1, 0.0, Some(NUM_FEATURES)).fit(trainFeaturized, labelGrabber(yTrain).get()) andThen TopKClassifier(1)


      val yPredTrain = model(trainFeaturized).get().map(x => x(0).toDouble)
      val yPredTest = model(testFeaturized).get().map(x => x(0).toDouble)

      println("TRAIN PREDICTIONS " + yPredTrain.count())
      println("TEST PREDICTIONS " + yPredTest.count())

      val evalTest = new BinaryClassificationMetrics(yPredTest.zip(yTest.map(_.toDouble)))
      println("Test Results: \n ")
      printMetrics(evalTest)
      */
    }
  }

  def printMetrics(metrics: BinaryClassificationMetrics) {
    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
  }

  def loadTsv(sc: SparkContext, filePath: String): RDD[(ReferenceRegion, Int)] = {
    val rdd = sc.textFile(filePath).filter(!_.contains("start"))
    rdd.map(line=> {
      val parts = line.split("\t")
      /* TODO: ignoring cell types for now */
      val label = parts.slice(3,parts.size).map(extractLabel(_)).reduceLeft(_ max _)
      (ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong), label)
    })

  }

  def timeElapsed(ns: Long) : Double = (System.nanoTime - ns).toDouble / 1e9

  def extractLabel(s: String): Int= {
    s match {
      case "A" => -1 // ambiguous
      case "U" => 0  // unbound
      case "B" => 1  // bound
      case _ => throw new IllegalArgumentException(s"Illegal label ${s}")
    }
  }
}
