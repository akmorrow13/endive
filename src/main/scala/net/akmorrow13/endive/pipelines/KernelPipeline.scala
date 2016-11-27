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
import java.io.File



object KernelPipeline  extends Serializable with Logging {

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


  def run(sc: SparkContext, conf: EndiveConf): Unit = {


    // set parameters
    val seed = 0
    val kmerSize = 8
    val dnaseSize = 100
    val approxDim = conf.dim
    val alphabetSize = Dataset.alphabet.size

    val dataPath = conf.aggregatedSequenceOutput
    val dnasePath = conf.dnaseNarrow
    val referencePath = conf.reference

    if (dataPath == null || referencePath == null || dnasePath == null) {
      println("Error: no data or reference path")
      sys.exit(-1)
    }

    // create sequence dictionary, used to save files
    val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
    val sd: SequenceDictionary = reference.sequences


    // load data for a specific transcription factor
    val allData: RDD[LabeledWindow] =
      LabeledWindowLoader(dataPath, sc).setName("_All data")
      	.cache()

    // extract tfs and cells for this label file
    val tfs = allData.map(_.win.getTf).distinct.collect()
    val cells = allData.map(_.win.getCellType).distinct.collect()

    println(s"running on TFs from ${dataPath}:")
    tfs.foreach(println)
    println(s"running on cells from ${dataPath}:")
    cells.foreach(println)

    // divvy up into train and test
    val (testCell, train, test) = {
      val first = allData.first
      val referenceName = first.win.getRegion.referenceName
      val cell = first.win.cellType

      // hold out a (chromosome, celltype) pair for test
      var train = allData.filter(r => (r.win.getRegion.referenceName != referenceName && r.win.cellType != cell))
      if (conf.sample) {
        // sample data
        train = EndiveUtils.subselectRandomSamples(sc, train, sd)
        println("Sampled train", train.count)
      }

      val test = allData.filter(r => (r.win.getRegion.referenceName == referenceName && r.win.cellType == cell))

      (cell, train, test)
    }

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val dnaseMax = train.map(r => r.win.dnase.max).max().toInt
    println("dnaseMax", dnaseMax)
    val W_sequence = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian)
    val W_dnase = DenseMatrix.rand(approxDim, dnaseSize * dnaseMax, gaussian)

    // generate approximation features
    val trainApprox = featurizeWithDnase(sc, train, sd, W_sequence, kmerSize)
	  .cache()
    val testApprox = featurizeWithDnase(sc, test, sd, W_sequence, kmerSize)
	  .cache()

    val labelExtractor = ClassLabelIndicatorsFromIntLabels(2) andThen
      new Cacher[DenseVector[Double]]

    val trainFeatures: RDD[DenseVector[Double]] = trainApprox.map(_.features)
    val trainLabels = labelExtractor(trainApprox.map(_.labeledWindow.label))

    val testFeatures = testApprox.map(_.features)
    val testLabels = labelExtractor(testApprox.map(_.labeledWindow.label))

    println(trainApprox.count, testApprox.count)

    // run least squares
    val predictor = new BlockLeastSquaresEstimator(approxDim, conf.epochs, conf.lambda).fit(trainFeatures, trainLabels.get)


    // train metrics
    val trainPredictions = MaxClassifier(predictor(trainFeatures)).map(_.toDouble)

    val prarr = trainPredictions.mapPartitions(iter => Array(iter.size).iterator, true).collect
    prarr.foreach(r => println(r))
    val approxarr = trainApprox.mapPartitions(iter => Array(iter.size).iterator, true).collect
    approxarr.foreach(r => println(r))


    val evalTrain = new BinaryClassificationMetrics(trainPredictions.zip(trainApprox.map(_.labeledWindow.label.toDouble)))
    println("Train Results: \n ")
    Metrics.printMetrics(evalTrain)

    // test metrics
    val testPredictions = MaxClassifier(predictor(testFeatures)).map(_.toDouble)
    val evalTest = new BinaryClassificationMetrics(testPredictions.zip(testApprox.map(_.labeledWindow.label.toDouble)))
    println("Test Results: \n ")
    Metrics.printMetrics(evalTest)

    // save train predictions as FeatureRDD if specified
    if (conf.saveTrainPredictions != null) {
      if (sd == null) {
        println("Error: need referencePath to save output train predictions")
      } else {
        saveAsFeatures(trainApprox.map(_.labeledWindow), trainPredictions, sd, conf.saveTrainPredictions)
      }
    }

    // save test predictions as FeatureRDD if specified
    if (conf.saveTestPredictions != null) {
      if (sd == null) {
        println("Error: need referencePath to save output train predictions")
      } else {
        saveAsFeatures(testApprox.map(_.labeledWindow), testPredictions, sd, conf.saveTestPredictions)
      }
    }
  }

  /**
   * Takes a region of the genome and one hot enodes sequences (eg A = 0001). If DNASE is enabled positive strands
   * are appended to the one hot sequence encoding. For example, if positive 1 has A with DNASE count of 3 positive
   * strands, the encoding is 0003.
   *
   * @param matrix: data matrix with sequences
   * @param W: random matrix
   * @param kmerSize: length of kmers to be created
   */
  def featurize(matrix: RDD[LabeledWindow],
                            W: DenseMatrix[Double],
                            kmerSize: Int): RDD[BaseFeature] = {

    val kernelApprox = new KernelApproximator(W, Math.cos, ngramSize = kmerSize)

    matrix.map(f => {
      val kx = (kernelApprox({
        KernelApproximator.stringToVector(f.win.sequence)
      }))
      BaseFeature(f, kx)
    })

  }

  /**
   * Takes a region of the genome and one hot enodes sequences (eg A = 0001). If DNASE is enabled positive strands
   * are appended to the one hot sequence encoding. For example, if positive 1 has A with DNASE count of 3 positive
   * strands, the encoding is 0003.
   *
   * @param sc: Spark Context
   * @param rdd: data matrix with sequences
   * @param sd: Sequence Dictionary
   * @param kmerSize size of kmers
   * @param W_sequence random matrix for sequence
   * @param W_dnase random matrix for dnase
   * @param dnaseSize size of dnase
   * @return
   */
  def featurizeWithDnase(sc: SparkContext,
                         rdd: RDD[LabeledWindow],
                         sd: SequenceDictionary,
                         W_sequence: DenseMatrix[Double],
                         kmerSize: Int,
                         W_dnase: Option[DenseMatrix[Double]] = None,
                         dnaseSize: Option[Int] = None): RDD[BaseFeature] = {

    val kernelApprox_seq = new KernelApproximator(W_sequence, Math.cos, ngramSize = kmerSize)

    if (W_dnase.isDefined && dnaseSize.isDefined) {
      val kernelApprox_dnase = new KernelApproximator(W_dnase.get, Math.cos, ngramSize = dnaseSize.get)
      rdd.map(f => {
        val k_seq = kernelApprox_seq(KernelApproximator.stringToVector(f.win.sequence))
        val k_dnase_pos = kernelApprox_dnase(f.win.dnase.slice(0, Dataset.windowSize))
        // TODO: include
        // val k_dnase_neg = kernelApprox_dnase(f.win.dnase.slice(Dataset.windowSize, f.win.dnase.length))

        BaseFeature(f, DenseVector.vertcat(k_seq, k_dnase_pos))
      })
    } else {
      rdd.map(f => {
        val kx = kernelApprox_seq(oneHotEncodeDnase(f))
        BaseFeature(f, kx)
      })
    }


  }

  /**
   * One hot encodes sequences with dnase data
   * @param f feature with sequence and featurized dnase
   * @return Densevector of sequence and dnase mushed together
   */
  private[pipelines] def oneHotEncodeDnase(f: LabeledWindow): DenseVector[Double] = {

      // form seq of int from bases and join with dnase
      val intString: Seq[(Int, Double)] = f.win.sequence.map(r => Dataset.alphabet.get(r).getOrElse(-1))
        .zip(f.win.dnase.slice(0, Dataset.windowSize).toArray) // slice off just positives

      val seqString = intString.map { r =>
        val out = DenseVector.zeros[Double](Dataset.alphabet.size)
        if (r._1 != -1) {
          out(r._1) = 1 + r._2
        }
        out
      }
      DenseVector.vertcat(seqString: _*)
  }

  /**
   * Saves predictions and labeled windows as a FeatureRDD.
   * @param labeledWindows RDD of windows and labels
   * @param predictions RDD of double predictions
   * @param sd SequenceDictionary required to create FeatureRDD
   * @param path path to save FeatureRDD
   */
  def saveAsFeatures(labeledWindows: RDD[LabeledWindow],
                     predictions: RDD[Double],
                     sd: SequenceDictionary,
                     path: String): Unit = {
    val features =
      labeledWindows.zip(predictions)
        .map(r => {
          Feature.newBuilder()
            .setPhase(r._1.label)
            .setScore(r._2)
            .setContigName(r._1.win.region.referenceName)
            .setStart(r._1.win.region.start)
            .setEnd(r._1.win.region.end)
            .build()
        })

    val featureRDD = FeatureRDD(features, sd)
    featureRDD.save(path, false)
  }
}
