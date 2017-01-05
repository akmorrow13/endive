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
import com.github.fommil.netlib.BLAS
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.RandomDistribution
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning._
import nodes.learning.{ BlockLeastSquaresEstimator}
import nodes.util.{Cacher, MaxClassifier, ClassLabelIndicatorsFromIntLabels}
import org.apache.commons.math3.random.MersenneTwister
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging

import breeze.numerics._
import breeze.stats._
import java.io.{File, BufferedWriter, FileWriter}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.Random
import nodes.stats._
import nodes.util._
import scala.collection.mutable.ArrayBuffer


object SolverHardNegativeThreshPipeline extends Serializable with Logging {

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
      val conf = new SparkConf().setAppName(appConfig.expName)
      conf.setIfMissing("spark.master", "local[4]")
      Logger.getLogger("org").setLevel(Level.INFO)
      Logger.getLogger("akka").setLevel(Level.INFO)
      val sc = new SparkContext(conf)
      val blasVersion = BLAS.getInstance().getClass().getName()
      println(s"Currently used version of blas is ${blasVersion}")
      run(sc, appConfig)
    }
  }

  def trainValSplit(featuresRDD: RDD[FeaturizedLabeledWindow], valChromosomes: Array[String], valCellTypes: Array[Int], valDuringSolve: Boolean, numPartitions: Int): (RDD[FeaturizedLabeledWindow], Option[RDD[FeaturizedLabeledWindow]])  = {
      // hold out a (chromosome, celltype) pair for val
      val trainFeaturizedWindows = featuresRDD.filter { r => 
                  val chrm:String = r.labeledWindow.win.getRegion.referenceName
                  val label:Int = r.labeledWindow.label
                  val cellType:Int = r.labeledWindow.win.cellType.id
                  !valChromosomes.contains(chrm)  && !valCellTypes.contains(cellType) && label != - 1
      }
      val valFeaturizedWindowsOpt =
        if (valDuringSolve) {
          println(featuresRDD.first.labeledWindow.win.cellType.id)
          println(featuresRDD.first.labeledWindow.win.getRegion.referenceName)
          println(valChromosomes.mkString(","))
          println(valCellTypes.mkString(","))
          val valFeaturizedWindows = featuresRDD.filter { r => 
                      val chrm:String = r.labeledWindow.win.getRegion.referenceName
                      val label:Int = r.labeledWindow.label
                      val cellType:Int = r.labeledWindow.win.cellType.id
                      valChromosomes.contains(chrm)  && valCellTypes.contains(cellType) && label != -1
          }
          Some(valFeaturizedWindows)
        } else {
          None
        }
      (trainFeaturizedWindows, valFeaturizedWindowsOpt)
  }


  def run(sc: SparkContext, conf: EndiveConf): Unit = {

    println(conf.featuresOutput)
    println("RUNNING NEGATIVE THRESHOLDING")
    var featuresRDD = FeaturizedLabeledWindowLoader(conf.featuresOutput, sc).cache()
    println(featuresRDD.first.labeledWindow.win.cellType.id)
    println(featuresRDD.first.labeledWindow.win.getRegion.referenceName)



    var (trainFeaturizedWindows, valFeaturizedWindowsOpt)  = trainValSplit(featuresRDD, conf.valChromosomes, conf.valCellTypes, conf.valDuringSolve, conf.numPartitions)

    val labelExtractor = ClassLabelIndicatorsFromIntLabels(2) andThen
      new Cacher[DenseVector[Double]]

      val numTestPositives = valFeaturizedWindowsOpt.map(_.filter(_.labeledWindow.label > 0).count()).getOrElse(0.0)
      val numTestNegatives = valFeaturizedWindowsOpt.map(_.filter(_.labeledWindow.label == 0).count()).getOrElse(0.0)


    val positives = trainFeaturizedWindows.filter(_.labeledWindow.label > 0).cache()
    val negativesFull = trainFeaturizedWindows.filter(_.labeledWindow.label == 0).cache()
    val rand = new Random(conf.seed)

    var trainFeaturizedWindowsSampled =
    if (conf.negativeSamplingFreq != 1.0) {
      val samplingIndices = (0 until negativesFull.count().toInt).map(x =>  (x, rand.nextFloat() < conf.negativeSamplingFreq)).filter(_._2).map(_._1).toSet
      val samplingIndicesB = sc.broadcast(samplingIndices)
      var negativesSampled = negativesFull.zipWithIndex.filter(x => samplingIndicesB.value contains x._2.toInt).map(x => x._1)
      positives.union(negativesSampled).repartition(conf.numPartitions).cache()
    } else {
      trainFeaturizedWindows
    }

    val numInputFeatures = 4
    val numOutputFeatures = 256
    val gamma = 1e-7
    val W = DenseMatrix.rand(numOutputFeatures, numInputFeatures,  Rand.gaussian) :* gamma
    val b = DenseVector.rand(numOutputFeatures,  Rand.uniform) :* (2*math.Pi)

    def refeaturize(y:FeaturizedLabeledWindow) = {

      val dnase = y.labeledWindow.win.dnase
      val features = y.features
      val seq = KernelApproximator.stringToVector(y.labeledWindow.win.sequence)

      val meanDnase = mean(dnase)
      val varDnase = variance(dnase)

      val meanSequence = mean(dnase)
      val varSequence = variance(dnase)

      val meanSeq = mean(seq)
      val varSeq = variance(seq)
      val simpleFeatures = DenseVector(meanDnase, varDnase, meanSequence, varSequence)
      val simpleFeaturesLift = (simpleFeatures.t * W.t).t
      simpleFeaturesLift :+= b
      simpleFeatures
    }


    val trainFeatures = trainFeaturizedWindowsSampled map refeaturize

    val allData = valFeaturizedWindowsOpt.get.map(x => x.labeledWindow)

    val numPositives = allData.filter({ x => x.label == 1}).count()
    val numNegatives = allData.filter({ x => x.label == 0}).count()

    println("NUM train POSITIVES with small variance DNASE " + allData.filter({ x =>
      x.label == 1 && variance(x.win.dnase) <= 0.005}).count())

    println("NUM train NEGATIVES with small variance DNASE " + allData.filter({ x =>
      x.label == 0 && variance(x.win.dnase) <= 0.005}).count())

    println("NUM val NEGATIVES " + numNegatives)
    println("NUM val POSITIVES " + numPositives)

    val valFeaturizedWindows = valFeaturizedWindowsOpt.get
    println("VAL COUNT " + valFeaturizedWindows.count())


    val valFeatures = valFeaturizedWindows map refeaturize

    val valLabels = valFeaturizedWindows.map(_.labeledWindow.label)
    val trainLabels = trainFeaturizedWindowsSampled.map(_.labeledWindow.label)


    val trainVectorLabels = labelExtractor(trainFeaturizedWindowsSampled.map(_.labeledWindow.label)).get
    val d = trainFeatures.first.size
    val model = new BlockLeastSquaresEstimator(d, 1, conf.lambda).fit(trainFeatures, trainVectorLabels)
    var valScores:RDD[Double] = model(valFeatures).map(x => x(1))
    var trainScores:RDD[Double] = model(trainFeatures).map(x => x(1))

    print("VALIDATION RESULTS ")
    val evalTest = new BinaryClassificationMetrics(valScores.zip(valLabels.map(_.toDouble)))

    Metrics.printMetrics(evalTest)

    val evalTrain = new BinaryClassificationMetrics(trainScores.zip(trainLabels.map(_.toDouble)))

    print("TRAIN RESULTS ")
    Metrics.printMetrics(evalTrain)

    val trainPredictionsOutput = conf.predictionsOutput + "/trainPreds"
    println("WRITING TRAIN PREDICTIONS TO DISK")
    val zippedTrainPreds = trainLabels.zip(trainScores).map(x => s"${x._1},${x._2}").saveAsTextFile(trainPredictionsOutput)

    val valPredictionsOutput = conf.predictionsOutput + s"/valPreds_${conf.valChromosomes.mkString(','.toString)}_${conf.valCellTypes.mkString(','.toString)}"
    val zippedValPreds = valLabels.zip(valScores).map(x => s"${x._1},${x._2}").saveAsTextFile(valPredictionsOutput)

    println("Saving model to disk")
    saveModel(model, conf.modelOutput)
  }


def saveModel(model: BlockLinearMapper, outLoc: String)  {
      val xs = model.xs.zipWithIndex
      xs.map(mi => breeze.linalg.csvwrite(new File(s"${outLoc}/model.weights.${mi._2}"), mi._1, separator = ','))
      model.bOpt.map(b => breeze.linalg.csvwrite(new File(s"${outLoc}/model.intercept"),b.toDenseMatrix, separator = ','))

   // Need to save scalers
    model.featureScalersOpt.map { scalers =>
      scalers.zipWithIndex.map { scaler =>
        val stdScaler: StandardScalerModel = scaler._1 match { 
            case x:StandardScalerModel => x
            case _ => {
                throw new Exception("Unsuported Scaler")
              }
            }
            println("SCALER MEAN IS " + stdScaler.mean.size)
            assert(stdScaler.std.isEmpty)
            breeze.linalg.csvwrite(new File(s"${outLoc}/model.scaler.mean.${scaler._2}"), stdScaler.mean.toDenseMatrix, separator = ',')
        }
    }
  }

def loadModel(modelLoc: String, blockSize: Int): BlockLinearMapper = {
    val files = ArrayBuffer.empty[Path]
    /* Hard coded rn */
    val numClasses = 2

    val root = Paths.get(modelLoc)

    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes) = {
        if (file.getFileName.toString.startsWith(s"${modelLoc}/model.weights")) {
          files += file
        }
        FileVisitResult.CONTINUE
      }
    })

    val xsPos: Seq[(Int, DenseMatrix[Double])] = files.map { f =>
      val modelPos = f.toUri.toString.split("\\.").takeRight(1)(0).toInt
      val xVector = loadDenseVector(f.toString)
      /* This is usually blocksize, but the last block may be smaller */
      val rows = xVector.size/numClasses
      val transposed = xVector.toDenseMatrix.reshape(numClasses, rows).t.toArray
      (modelPos, new DenseMatrix(rows, numClasses, transposed))
    }
    val xsPosSorted = xsPos.sortBy(_._1)
    val xs = xsPosSorted.map(_._2)
    val interceptPath = s"${modelLoc}/model.intercept"
    val bOpt =
      if (Files.exists(Paths.get(interceptPath))) {
        Some(loadDenseVector(interceptPath))
      } else {
        None
      }

    val scalerFiles = ArrayBuffer.empty[Path]
    Files.walkFileTree(root, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes) = {
        if (file.getFileName.toString.startsWith(s"${modelLoc}/model.scaler.mean")) {
          scalerFiles += file
        }
        FileVisitResult.CONTINUE
      }
    })

    val scalerPos: Seq[(Int, StandardScalerModel)] = files.map { f =>
      val scalerPos = f.toUri.toString.split("\\.").takeRight(1)(0).toInt
      val mean = loadDenseVector(f.toString)
      val scaler = new StandardScalerModel(mean)
      (scalerPos, scaler)
    }

    val scalerPosSorted = scalerPos.sortBy(_._1)
    val scalers = Some(scalerPosSorted.map(_._2)).filter(_.size > 0)
    new BlockLinearMapper(xs, blockSize, bOpt, scalers)
  }

 def loadDenseVector(path: String): DenseVector[Double] = {
    DenseVector(scala.io.Source.fromFile(path).getLines.toArray.flatMap(_.split(",")).map(_.toDouble))
  }



}

