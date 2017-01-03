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

import java.util.Random

import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.RandomDistribution
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import com.github.fommil.netlib.BLAS
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.{ BlockLeastSquaresEstimator}
import nodes.util.{Cacher, MaxClassifier, ClassLabelIndicatorsFromIntLabels}
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
import org.apache.commons.math3.random.MersenneTwister
import nodes.learning._

import java.io.{File, BufferedWriter, FileWriter}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import scala.collection.mutable.ArrayBuffer
import nodes.stats._
import nodes.util._


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
                      valChromosomes.contains(chrm)  && valCellTypes.contains(cellType)
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
    var featuresRDD = FeaturizedLabeledWindowLoader(conf.featuresOutput, sc)

    var (trainFeaturizedWindows, valFeaturizedWindowsOpt)  = trainValSplit(featuresRDD, conf.valChromosomes, conf.valCellTypes, conf.valDuringSolve, conf.numPartitions)
    trainFeaturizedWindows.cache()
    valFeaturizedWindowsOpt.get.cache()

    val labelExtractor = ClassLabelIndicatorsFromIntLabels(2) andThen
      new Cacher[DenseVector[Double]]

    val numTestPositives = valFeaturizedWindowsOpt.map(_.filter(_.labeledWindow.label > 0).count()).getOrElse(0.0)
    val numTestNegatives = valFeaturizedWindowsOpt.map(_.filter(_.labeledWindow.label == 0).count()).getOrElse(0.0)

    println(s"NUMBER OF TEST (POS, NEG) is ${numTestPositives}, ${numTestNegatives}")


    val positives = trainFeaturizedWindows.filter(_.labeledWindow.label > 0).cache()
    val posCount = positives.count

    val negativesFull = trainFeaturizedWindows.filter(_.labeledWindow.label == 0).cache()
    val negCount = negativesFull.count

    println(s"NUMBER OF TRAIN (POS, NEG) is ${posCount}, ${negCount}")

    val negativesSampled =
    if (conf.negativeSamplingFreq < 1.0) {
      val rand = new Random(conf.seed)

      val samplingIndices = (0 until negCount.toInt).map(x =>  (x, rand.nextFloat() < conf.negativeSamplingFreq)).filter(_._2).map(_._1).toSet
      val samplingIndicesB = sc.broadcast(samplingIndices)
      val negativesSampled = negativesFull.zipWithIndex.filter(x => samplingIndicesB.value contains x._2.toInt).map(x => x._1)
      negativesSampled
    } else negativesFull

    println("NUM SAMPLED NEGATIVES " + negativesSampled.count())
    println("NUM FULL SAMPLED NEGATIVES " + negCount)
    val valFeaturizedWindows = valFeaturizedWindowsOpt.get
    val valFeatures = valFeaturizedWindows.map(_.features)

    var trainFeaturizedWindowsSampled = positives.union(negativesSampled).repartition(conf.numPartitions).cache()
    println(s"train features: ${trainFeaturizedWindowsSampled.count}")
    
    trainFeaturizedWindows.unpersist()

    var i = 0;
    val models:Seq[BlockLinearMapper] =
    (0 until conf.numItersHardNegative).map({ x => 
          val trainFeatures = trainFeaturizedWindowsSampled.map(_.features)
          val trainLabels = labelExtractor(trainFeaturizedWindowsSampled.map(_.labeledWindow.label)).get
          val d = trainFeatures.first.size
          val model = new BlockLeastSquaresEstimator(d, 1, conf.lambda).fit(trainFeatures, trainLabels)

          // get train missed positives and negativs
          val negPredictions = model(negativesFull.map(_.features))
          val posPredictions = model(positives.map(_.features))

          val negMax: RDD[Int] = MaxClassifier(negPredictions)
          val posMax: RDD[Int] = MaxClassifier(posPredictions)

          val missedPos: RDD[FeaturizedLabeledWindow] = positives.zip(posMax).filter(_._2 == 0).map(_._1)
          val missedNegs: RDD[FeaturizedLabeledWindow] = negativesFull.zip(negMax).filter(_._2 == 1).map(_._1)
              .sample(false, 10000)

          val missed = missedPos.union(missedNegs)

          val valPredictions = model(valFeatures)
          val predictionsVal: RDD[Int] = MaxClassifier(valPredictions)
          val valResults = predictionsVal.zip(valFeaturizedWindows)
          valResults.setName("valResults").cache()
          valResults.count()

          val valMissedPos = valResults.filter({x => x._2.labeledWindow.label == 1 && x._1 == 0}).count()
          val valMissedNegs = valResults.filter({x => x._2.labeledWindow.label == 0 && x._1 == 1}).count()

          trainFeaturizedWindowsSampled = trainFeaturizedWindowsSampled.union(missed).repartition(conf.numPartitions)
          trainFeaturizedWindowsSampled.setName("trainFeaturizedWindowsSampled").cache()

          println(s"neg Iteration: ${i}, missed neg: ${missedNegs.count()} out of ${negCount}, " +
            s"missed pos: ${missedPos.count} out of ${posCount}, " +
            s"missed pos(val): ${valMissedPos} out of ${numTestPositives}" +
            s", missed neg(val) ${valMissedNegs} out of ${numTestNegatives}")
          i = i + 1

          val trainZipped = negPredictions.zip(negativesFull)
            .union(posPredictions.zip(positives))
            .map(r => (r._1(1), r._2.labeledWindow.label.toDouble))

          val evalTrain = new BinaryClassificationMetrics(trainZipped)
          println("train eval")
          Metrics.printMetrics(evalTrain)

          val evalTest = new BinaryClassificationMetrics(valPredictions.map(x => x(1)).zip(valFeaturizedWindows.map(_.labeledWindow.label.toDouble)))
	        println("test eval")
          Metrics.printMetrics(evalTest)

          valResults.unpersist()
          model
    })

    val model = models(models.size - 1)
    val trainFeatures = trainFeaturizedWindows.map(_.features)
    val trainLabels = labelExtractor(trainFeaturizedWindows.map(_.labeledWindow.label)).get

    if (conf.valDuringSolve) {
      val trainPredictions:RDD[Double] = model(trainFeatures).map(x => x(1))
      trainPredictions.count()
      val trainScalarLabels = trainLabels.map(x => if(x(1) == 1) 1 else 0)
      val trainPredictionsOutput = conf.predictionsOutput + "/trainPreds"
      println("WRITING TRAIN PREDICTIONS TO DISK")
      val zippedTrainPreds = trainScalarLabels.zip(trainPredictions).map(x => s"${x._1},${x._2}").saveAsTextFile(trainPredictionsOutput)
      val valFeaturizedWindows = valFeaturizedWindowsOpt.get
      val valFeatures = valFeaturizedWindows.map(_.features)
      var valPredictions: RDD[Double] = model(valFeatures).map(x => x(1))
      val (min, max) = (valPredictions.min(), valPredictions.max())
      valPredictions = valPredictions.map(r => ((r - min)/(max - min)))

      valPredictions.count()
      val valScalarLabels = valFeaturizedWindows.map(_.labeledWindow.label)
      val valPredictionsOutput = conf.predictionsOutput + s"/valPreds_${conf.valChromosomes.mkString(','.toString)}_${conf.valCellTypes.mkString(','.toString)}"
      val zippedValPreds = valScalarLabels.zip(valPredictions).map(x => s"${x._1},${x._2}").saveAsTextFile(valPredictionsOutput)

      try {
        // save test predictions if specified
        println(conf.saveTestPredictions)
        if (conf.saveTestPredictions != null) {
          val first = valFeaturizedWindows.first.labeledWindow.win
          val cellType = first.getCellType.toString
          val tf = first.getTf.toString
          val chr = first.getRegion.referenceName

          // create sequence dictionary, used to save files
          val reference = new TwoBitFile(new LocalFileByteAccess(new File(conf.reference)))
          val sd: SequenceDictionary = reference.sequences
          println(sd)
          SolverPipeline.saveAsFeatures(valFeaturizedWindows.map(_.labeledWindow).zip(valPredictions).filter(_._2 >= 0.5),
            sd, conf.saveTrainPredictions + s"${tf}_${cellType}_${chr}_predicted.bed")
          SolverPipeline.saveAsFeatures(valFeaturizedWindows.map(_.labeledWindow).map(r => (r, r.label.toDouble)).filter(_._2 >= 0.5),
            sd, conf.saveTrainPredictions + s"${tf}_${cellType}_${chr}_true.bed")
        }
      } catch {
        case e: Exception => {
          println("failed saving output predicted features")
          println(e.getMessage)
        }
      }
    }

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

