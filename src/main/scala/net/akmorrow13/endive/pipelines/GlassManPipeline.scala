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
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.Sequence
import net.akmorrow13.endive.processing.Dataset.{TranscriptionFactors, CellTypes}
import net.akmorrow13.endive.utils._
import net.jafama.FastMath
import nodes.learning._
import nodes.nlp._
import nodes.stats._
import nodes.util._

import com.github.fommil.netlib.BLAS
import evaluation.BinaryClassifierEvaluator
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
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
import utils.{Image, MatrixUtils, Stats, ImageMetadata, LabeledImage, RowMajorArrayVectorizedImage, ChannelMajorArrayVectorizedImage}
import workflow.{Pipeline, Transformer}
import org.apache.commons.math3.random.MersenneTwister
import java.io.{File, BufferedWriter, FileWriter}


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
      Logger.getLogger("org").setLevel(Level.INFO)
      Logger.getLogger("akka").setLevel(Level.INFO)
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
      val out = DenseVector.zeros[Double](4)
      if (bp != -1) {
        out(bp) = 1
      }
      out
    }
    DenseVector.vertcat(seqString:_*)
  }

  def vectorToString(in: DenseVector[Double], alphabetSize: Int): String = {

   val BASEPAIRREVMAP = Array('A', 'T', 'C', 'G')
   var i = 0
   var str = ""
   while (i < in.size) {
    val charVector = in(i until i+alphabetSize)
    if (charVector == DenseVector.zeros[Double](alphabetSize)) {
      str += "N"
    } else {
      val bp = BASEPAIRREVMAP(argmax(charVector))
      str += bp
    }
    i += alphabetSize
   }
   str
  }






  def run(sc: SparkContext, conf: EndiveConf) {
    val dataTxtRDD:RDD[String] = sc.textFile(conf.aggregatedSequenceOutput, minPartitions=600)

    val allData:RDD[LabeledWindow] = LabeledWindowLoader(conf.aggregatedSequenceOutput, sc).setName("_All data").cache()
    println("RAW WINDOW COUNT " + allData.count())
    println("RAW NUM POSITIVES " + allData.filter(_.label == 1).count())
    println("RAW NUM NEGATIVES " + allData.filter(_.label == 0).count())
    val r = new java.util.Random(0)
    val filteredData = allData.filter(x => x.label == 1 || (x.label == 0 && r.nextFloat < 0.005)).setName("Filtered Data").repartition(300).cache()
    println("FILTERED WINDOW COUNT " + filteredData.count())
    println("FILTERED NUM POSITIVES " + filteredData.filter(_.label == 1).count())
    println("FILTERED NUM NEGATIVES " + filteredData.filter(_.label == 0).count())
    println("DATA LOADED filtered AND REPARTIONED")
    val labelVectorizer = ClassLabelIndicatorsFromIntLabels(2)
    val foldsData = EndiveUtils.generateFoldsRDD(filteredData.keyBy(r => (r.win.region.referenceName, r.win.cellType)), numFolds = 5, numHeldOutChromosomes = 5, numHeldOutCellTypes = 2, randomSeed = 3)
    val cellTypeFeaturizer = Transformer.apply[LabeledWindow, Int](x => x.win.cellType.id) andThen new ClassLabelIndicatorsFromIntLabels(CellTypes.toVector.size)
    foldsData.map(_._1.count())
    foldsData.map(_._2.count())
    println("FOLDS DATA GENERATED")
    val ngramSize = 8
    val alphabetSize = 4
    val approxDim = 4096
    val seed = 0

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)
    val uniform = new Uniform(0, 1)
    val W = 0.1 * DenseMatrix.rand(approxDim, ngramSize*alphabetSize, gaussian)
    val b = DenseVector.rand(approxDim, uniform)
    for (i <- (0 until 4)) {

      println("Fold " + i)
      //println("HELD OUT CELL TYPES " + foldsData(i)._3.mkString(","))
      //println("HELD OUT CHROMOSOMES " + foldsData(i)._4.mkString(","))
      //val foldId = "EGR1_fold_" + i + "_" + foldsData(i)._3.mkString("-") + "_" + foldsData(i)._4.mkString("-")
      //println("Fold Id IS " + foldId)

      var train = foldsData(i)._1.map(x => x._2)
      val test = foldsData(i)._2.map(x => x._2)

      var XTrain:RDD[DenseVector[Double]] = train.map(x => denseFeaturize(x.win.sequence)).setName("XTrain").cache()

      val XTest:RDD[DenseVector[Double]] = test.map(x => denseFeaturize(x.win.sequence)).setName("XTest").cache()


      XTrain.count()
      XTest.count()

      val yTrain = train.map(_.label).setName("yTrain").cache()
      val yTest = test.map(_.label).setName("yTest").cache()

      println("Collecting matrices")
      val XTrainLocal:DenseMatrix[Double] = MatrixUtils.rowsToMatrix(XTrain.collect())
      val XTestLocal:DenseMatrix[Double] = MatrixUtils.rowsToMatrix(XTest.collect())

      val yTrainLocal:DenseMatrix[Double] = convert(DenseVector(yTrain.collect()).toDenseMatrix, Double)
      val yTestLocal:DenseMatrix[Double] = convert(DenseVector(yTest.collect()).toDenseMatrix, Double)

      //val XTrainFile  = new File(s"/home/eecs/vaishaal/endive-data/${foldId}_Train.csv")
      //val XTestFile = new File(s"/home/eecs/vaishaal/endive-data/${foldId}_Test.csv")

      //val yTrainFile = new File(s"/home/eecs/vaishaal/endive-data/${foldId}_TrainLabels.csv")
      //val yTestFile = new File(s"/home/eecs/vaishaal/endive-data/${foldId}_TestLabels.csv")

//      csvwrite(XTrainFile, XTrainLocal)
//      csvwrite(XTestFile, XTestLocal)
//      csvwrite(yTrainFile, yTrainLocal)
//      csvwrite(yTestFile, yTestLocal)



      /*
      println("Converted data to one hot representation")
      println("Generating random lift now...")


      val kernelApprox = new KernelApproximator(filters=W)

      val XTrainLift = kernelApprox(XTrain).setName("XTrainLift").cache()
      val XTestLift = kernelApprox(XTest).setName("XTestLift").cache()


      val trainCount = XTrainLift.count()
      val testCount = XTestLift.count()

      println(s"Fold ${i}, training points ${trainCount}, testing points ${testCount}")

      println("Fitting model now...")
      val labelVectorizer = ClassLabelIndicatorsFromIntLabels(2)
      val model = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 10, regParam=0.0).fit(XTrainLift, yTrain)

      val yPredTrain = model(XTrainLift)
      val yPredTest = model(XTestLift)

      val evalTrain = new BinaryClassificationMetrics(yPredTrain.zip(yTrain.map(_.toDouble)))
      val evalTest = new BinaryClassificationMetrics(yPredTest.zip(yTest.map(_.toDouble)))
      println("Train Results (lifted): \n ")
      printMetrics(evalTrain)

      println("Test Results (lifted): \n ")
      printMetrics(evalTest)

      println("Training Baseline model")
      val modelBaseLine = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 10, regParam=0.0).fit(XTrain, yTrain)

      val yPredTrainBaseLine = modelBaseLine(XTrain)
      val yPredTestBaseLine = modelBaseLine(XTest)

      val evalTrainBaseLine = new BinaryClassificationMetrics(yPredTrainBaseLine.zip(yTrain.map(_.toDouble)))
      val evalTestBaseLine = new BinaryClassificationMetrics(yPredTestBaseLine.zip(yTest.map(_.toDouble)))


      println("Train Results (baseLine): \n ")
      printMetrics(evalTrainBaseLine)

      println("Test Results (baseLine): \n ")
      printMetrics(evalTestBaseLine)
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
