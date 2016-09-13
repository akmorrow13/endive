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
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing.CellTypes
import net.akmorrow13.endive.utils._
import nodes.learning._
import nodes.nlp._
import nodes.stats.TermFrequency
import nodes.util.CommonSparseFeatures
import nodes.util.{Identity, Cacher, ClassLabelIndicatorsFromIntLabels, TopKClassifier, MaxClassifier, VectorCombiner}
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

object StrawmanPipeline extends Serializable with Logging {

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

    val foldsData = allData.map(x => (x.win.getRegion.referenceName.hashCode() % conf.folds, x))
    val labelVectorizer = ClassLabelIndicatorsFromIntLabels(2)

    for (i <- (0 until conf.folds)) {
      val r = new java.util.Random()
      var train = foldsData.filter(x => x._1 != i).filter(x => x._2.label == 1 || (x._2.label == 0 && r.nextFloat < 0.001)).map(x => x._2).setName("train").cache()
      train.count()
      val test = foldsData.filter(x => x._1 == i).map(x => x._2).setName("test").cache()
      val yTrain = train.map(_.label).setName("yTrain").cache()
      val yTest = test.map(_.label).setName("yTest").cache()

      println(s"Fold ${i}, training points ${train.count()}, testing points ${test.count()}")

      /* Make an estimator? */
      val cellTypes:Map[CellTypes.Value, Int] = train.map(x => (x.win.cellType)).countByValue().keys.zipWithIndex.toMap
      val cellTypeFeaturizer = Transformer.apply[LabeledWindow, Int](x => cellTypes(x.win.cellType)) andThen new ClassLabelIndicatorsFromIntLabels(cellTypes.size)


      println("Building Pipeline")
      val sequenceFeaturizer =
      Transformer.apply[LabeledWindow, DenseVector[Double]](x => denseFeaturize(x.win.sequence)) andThen Cacher[DenseVector[Double]]()


      val predictor = Pipeline.gather[LabeledWindow, DenseVector[Double]] {
        sequenceFeaturizer :: (cellTypeFeaturizer) :: Nil
       } andThen Transformer.apply[Seq[DenseVector[Double]], DenseVector[Double]](x => x.reduce(DenseVector.vertcat(_,_))) andThen (LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 10, regParam=0.1), train, yTrain)


      val yPredTrain = predictor(train).get()
      val evalTrain = new BinaryClassificationMetrics(yPredTrain.zip(yTrain.map(_.toDouble)))
      println("Train Results: \n ")
      Metrics.printMetrics(evalTrain)


      val yPredTest = predictor(test).get()
      val evalTest = new BinaryClassificationMetrics(yPredTest.zip(yTest.map(_.toDouble)))
      println("Test Results: \n ")
      Metrics.printMetrics(evalTest)
    }
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
