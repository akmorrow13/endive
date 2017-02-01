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



object SolverPipeline extends Serializable with Logging {

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

  /**
   * Holds out a (chromosome, celltype) pair for evaluation
   * @param featuresRDD Features
   * @param valChromosomes All chromosomes
   * @param valCellTypes All CellTypes
   * @param valDuringSolve Whether or not to evalut
   * @return train and test RDDs
   */
  def trainValSplit(featuresRDD: RDD[FeaturizedLabeledWindow],
                    valChromosomes: Array[String],
                    valCellTypes: Array[Int]): (RDD[FeaturizedLabeledWindow], RDD[FeaturizedLabeledWindow])  = {
    // hold out a (chromosome, celltype) pair for val
    val trainFeaturizedWindows = featuresRDD.filter { r =>
      val chr = r.labeledWindow.win.getRegion.referenceName
      val label = r.labeledWindow.label
      val cellType = r.labeledWindow.win.getCellType.id
      !valChromosomes.contains(chr)  && !valCellTypes.contains(cellType) && label != - 1
    }
    val valFeaturizedWindows = featuresRDD.filter { r =>
      val chr = r.labeledWindow.win.getRegion.referenceName
      val label = r.labeledWindow.label
      val cellType = r.labeledWindow.win.getCellType.id
      valChromosomes.contains(chr) && valCellTypes.contains(cellType)
    }
    (trainFeaturizedWindows, valFeaturizedWindows)
  }


  def run(sc: SparkContext, conf: EndiveConf): Unit = {
    // load in data
    var featuresRDD = FeaturizedLabeledWindowLoader(conf.featuresOutput, sc)

    val negativeCount = featuresRDD.filter(_.labeledWindow.label == 0).count
    val positiveCount = featuresRDD.filter(_.labeledWindow.label == 0).count

    var samplingFreq = positiveCount.toDouble/negativeCount.toDouble

    while (samplingFreq <= 1.0) {

      // split into train and eval based on celltypes
      var (trainFeaturizedWindows, valFeaturizedWindowsOpt)  =
        if (conf.valDuringSolve) {
          val (train, eval) = trainValSplit(featuresRDD, conf.valChromosomes, conf.valCellTypes)
          (train, Some(eval))
        } else {
          (featuresRDD, None)
        }

      // if specified, sample negatives
      if (samplingFreq < 1.0) {
        println("NEGATIVE SAMPLING")
        val rand = new Random(conf.seed)
        val negativesFull = trainFeaturizedWindows.filter(_.labeledWindow.label == 0)
        val samplingIndices = (0 until negativesFull.count().toInt).map(x =>  (x, rand.nextFloat() < conf.negativeSamplingFreq)).filter(_._2).map(_._1).toSet
        val samplingIndicesB = sc.broadcast(samplingIndices)
        val negatives = negativesFull.zipWithIndex.filter(x => samplingIndicesB.value contains x._2.toInt).map(x => x._1)

        val positives = trainFeaturizedWindows.filter(_.labeledWindow.label > 0)
        trainFeaturizedWindows = positives.union(negatives)
      }

      trainFeaturizedWindows = trainFeaturizedWindows.repartition(conf.numPartitions).cache()
      valFeaturizedWindowsOpt = valFeaturizedWindowsOpt.map(_.repartition(conf.numPartitions).cache())

      val labelExtractor = ClassLabelIndicatorsFromIntLabels(2) andThen
        new Cacher[DenseVector[Double]]

      val trainFeatures = trainFeaturizedWindows.map(_.features)
      val trainLabels = labelExtractor(trainFeaturizedWindows.map(_.labeledWindow.label)).get

      // Solve Model, either using LeastSquares of Weighted LeastSquares
      val model =
        if (conf.mixtureWeight > 0) {
          new PerClassWeightedLeastSquaresEstimator(conf.approxDim, 1, conf.lambda, conf.mixtureWeight).fit(trainFeatures, trainLabels)
        } else {
          new BlockLeastSquaresEstimator(conf.approxDim, 1, conf.lambda).fit(trainFeatures, trainLabels)
        }


      if (conf.valDuringSolve) {
        val trainPredictions: RDD[Double] = model(trainFeatures).map(x => x(1))
        trainPredictions.count()
        val trainScalarLabels = trainLabels.map(x => if (x(1) == 1) 1 else 0)
        val trainPredictionsOutput = conf.predictionsOutput + s"/trainPreds_${samplingFreq}"
        println(s"WRITING TRAIN PREDICTIONS TO DISK AT ${trainPredictionsOutput}")
        val zippedTrainPreds = trainScalarLabels.zip(trainPredictions).map(x => s"${x._1},${x._2}").saveAsTextFile(trainPredictionsOutput)
        var valFeaturizedWindows = valFeaturizedWindowsOpt.get
        // filter out dnase with < mean
        valFeaturizedWindows = valFeaturizedWindows //.filter(_.labeledWindow.win.dnase.sum > 1)
        //  val hardNegatives  = valFeaturizedWindows.filter(_.labeledWindow.win.dnase.sum <= 1).map(r => (r.labeledWindow.label, 0.0))

        val valFeatures = valFeaturizedWindows.map(_.features)
        var valPredictions: RDD[Double] = model(valFeatures).map(x => x(1))
        val (min, max) = (valPredictions.min(), valPredictions.max())
        valPredictions = valPredictions.map(r => ((r - min) / (max - min)))
        val valScalarLabels = valFeaturizedWindows.map(_.labeledWindow.label)
        val valPredictionsOutput = conf.predictionsOutput + s"/valPreds_${conf.valChromosomes.mkString(','.toString)}_${conf.valCellTypes.mkString(','.toString)}"

        val zippedValPreds = valScalarLabels.zip(valPredictions)
        zippedValPreds.map(x => s"${x._1},${x._2}").saveAsTextFile(s"${valPredictionsOutput}_${samplingFreq}")

        try {
          // save test predictions if specified
          if (conf.saveTestPredictions != null) {
            val first = valFeaturizedWindows.first.labeledWindow.win
            val cellType = first.getCellType.toString
            val tf = first.getTf.toString
            val chr = first.getRegion.referenceName

            // create sequence dictionary, used to save files
            val reference = new TwoBitFile(new LocalFileByteAccess(new File(conf.reference)))
            val sd: SequenceDictionary = reference.sequences
            println(sd)
            saveAsFeatures(valFeaturizedWindows.map(_.labeledWindow).zip(valPredictions).filter(_._2 >= 0.5),
              sd, conf.saveTrainPredictions + s"${tf}_${cellType}_${chr}_predicted.bed")
            saveAsFeatures(valFeaturizedWindows.map(_.labeledWindow).map(r => (r, r.label.toDouble)).filter(_._2 >= 0.5),
              sd, conf.saveTrainPredictions + s"${tf}_${cellType}_${chr}_true.bed")
          }
        } catch {
          case e: Exception => {
            println("failed saving output predicted features")
            println(e.getMessage)
          }
        }
      }
      samplingFreq = samplingFreq + 0.1
//      println("Saving model to disk")
//      saveModel(model, conf.modelOutput)
    }
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

  /**
   * Saves predictions and labeled windows as a FeatureRDD.
   * @param labeledWindows RDD of windows and labels
   * @param sd SequenceDictionary required to create FeatureRDD
   * @param path path to save FeatureRDD
   */
  def saveAsFeatures(labeledWindows: RDD[(LabeledWindow, Double)],
                     sd: SequenceDictionary,
                     path: String): Unit = {
    val features =
      labeledWindows
        .map(r => {
          Feature.newBuilder()
            .setFeatureId(s"${r._1.win.getRegion.toString}-score:${r._1.label}")
            .setPhase(r._1.label)
            .setScore(r._2 * 1000)
            .setContigName(r._1.win.getRegion.referenceName)
            .setStart(r._1.win.getRegion.start)
            .setEnd(r._1.win.getRegion.end)
            .build()
        })

    val featureRDD = new FeatureRDD(features, sd)
    println(s"saving features to path ${path}")
    featureRDD.save(path, false)
  }

}
