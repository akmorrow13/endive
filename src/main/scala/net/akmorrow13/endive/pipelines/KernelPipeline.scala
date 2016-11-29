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
import evaluation.BinaryClassificationMetrics
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import com.github.fommil.netlib.BLAS
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.{BlockLinearMapper, BlockLeastSquaresEstimator}
import nodes.util.{Cacher, MaxClassifier, ClassLabelIndicatorsFromIntLabels}
import org.apache.log4j.{Level, Logger}
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


import nodes.stats.{CosineRandomFeatures, StandardScaler}

class KernelPipeline(configFilepath: String) {
  val prediction = KernelPipeline(configFilepath)
}

object KernelPipeline extends EndiveLearningPipeline with Serializable with Logging {

  def apply(configFilepath: String): RDD[Double] = {
    val configfile = scala.io.Source.fromFile(configFilepath)
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
    runPrediction(sc, appConfig)
  }

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


  def run(sc: SparkContext, conf: EndiveConf) = {

    // set parameters
    val seed = 0
    val kmerSize = 8
    val approxDim = conf.dim
    val alphabetSize = Dataset.alphabet.size

    val dataPath = conf.aggregatedSequenceOutput
    val referencePath = conf.reference

    if (dataPath == null || referencePath == null) {
      println("Error: no data or reference path")
      sys.exit(-1)
    }

    // create sequence dictionary, used to save files
    val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
    val sd: SequenceDictionary = reference.sequences


    // load data for a specific transcription factor
    var allData: RDD[LabeledWindow] =
      LabeledWindowLoader(dataPath, sc).setName("_All data")
        .filter(_.label >= 0)
        .cache()

    if (conf.sample) {
      // sample data
      allData = EndiveUtils.subselectRandomSamples(sc, allData, sd)
      println("Sampled LabeledWindows", allData.count)
    }

    // extract tfs and cells for this label file
    val tfs = allData.map(_.win.getTf).distinct.collect()
    val cells = allData.map(_.win.getCellType).distinct.collect()

    println(s"running on TFs from ${dataPath}:")
    tfs.foreach(println)
    println(s"running on cells from ${dataPath}:")
    cells.foreach(println)

    val first = allData.first
    val referenceName = first.win.getRegion.referenceName
    val cell = first.win.cellType

    // hold out a (chromosome, celltype) pair for test
    val test = allData.filter(r => (r.win.getRegion.referenceName == referenceName && r.win.cellType == cell))

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val W = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian)

    // generate approximation features
    val testApprox = featurize(test, W, kmerSize).cache()
    val testFeatures = testApprox.map(_.features)

    // run least squares
    val predictor = loadModel(conf.modelPath).getOrElse(generateModel(conf, allData, referenceName, cell, W, kmerSize, sd))

  }

  def runPrediction(sc: SparkContext, conf: EndiveConf): RDD[Double] = {
    // set parameters
    val seed = 0
    val kmerSize = 8
    val approxDim = conf.dim
    val alphabetSize = Dataset.alphabet.size

    val dataPath = conf.aggregatedSequenceOutput
    val referencePath = conf.reference

    if (dataPath == null || referencePath == null) {
      println("Error: no data or reference path")
      sys.exit(-1)
    }

    // create sequence dictionary, used to save files
    val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
    val sd: SequenceDictionary = reference.sequences


    // load data for a specific transcription factor
    var allData: RDD[LabeledWindow] =
      LabeledWindowLoader(dataPath, sc).setName("_All data")
        .filter(_.label >= 0)
        .cache()

    if (conf.sample) {
      // sample data
      allData = EndiveUtils.subselectRandomSamples(sc, allData, sd)
      println("Sampled LabeledWindows", allData.count)
    }

    // extract tfs and cells for this label file
    val tfs = allData.map(_.win.getTf).distinct.collect()
    val cells = allData.map(_.win.getCellType).distinct.collect()

    println(s"running on TFs from ${dataPath}:")
    tfs.foreach(println)
    println(s"running on cells from ${dataPath}:")
    cells.foreach(println)

    val first = allData.first
    val referenceName = first.win.getRegion.referenceName
    val cell = first.win.cellType

    // hold out a (chromosome, celltype) pair for test
    val test = allData.filter(r => (r.win.getRegion.referenceName == referenceName && r.win.cellType == cell))

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val W = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian)

    // generate approximation features
    val testApprox = featurize(test, W, kmerSize).cache()
    val testFeatures = testApprox.map(_.features)

    // run least squares
    val predictor = loadModel(conf.modelPath).getOrElse(generateModel(conf, allData, referenceName, cell, W, kmerSize, sd))

    // test metrics
    MaxClassifier(predictor(testFeatures)).map(_.toDouble)
  }

  def generateModel(conf: EndiveConf, allData: RDD[LabeledWindow], referenceName: String, cell: CellTypes.Value, W: DenseMatrix[Double], kmerSize: Int, sd: SequenceDictionary): BlockLinearMapper = {

    val train = allData.filter(r => r.win.getRegion.referenceName != referenceName && r.win.cellType != cell)

    val trainApprox = featurize(train, W, kmerSize)
    val trainFeatures: RDD[DenseVector[Double]] = trainApprox.map(_.features)
    val labelExtractor = ClassLabelIndicatorsFromIntLabels(2) andThen
      new Cacher[DenseVector[Double]]
    val trainLabels = labelExtractor(trainApprox.map(_.labeledWindow.label))

    val predictor = new BlockLeastSquaresEstimator(conf.dim, conf.epochs, conf.lambda).fit(trainFeatures, trainLabels)
    val trainPredictions = MaxClassifier(predictor(trainFeatures)).map(_.toDouble)

    saveModel(conf.modelPath, predictor)
    predictor
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
  def featurizeStrings(matrix: Array[String],
                W: DenseMatrix[Double],
                kmerSize: Int): Array[DenseVector[Double]] = {

    val kernelApprox = new KernelApproximator(W, Math.cos, ngramSize = kmerSize)

    matrix.map(f => {
      kernelApprox({
        KernelApproximator.stringToVector(f)
      })
    })
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
   * @param rdd: data matrix with sequences
   * @param W: random matrix
   * @param kmerSize: length of kmers to be created
   */
  def featurizeWithDnase(sc: SparkContext,
                         rdd: RDD[LabeledWindow],
                         W: DenseMatrix[Double],
                         sd: SequenceDictionary,
                         kmerSize: Int,
                         cells: Array[CellTypes.Value],
                         dnasePath: String): RDD[BaseFeature] = {

    val kernelApprox = new KernelApproximator(W, Math.cos, ngramSize = kmerSize)

    // load cuts from AlignmentREcordRDD. filter out only cells of interest
    val dnase = Preprocess.loadDnase(sc, dnasePath, cells)
      .transform(rdd =>
        rdd.filter(r => !r.getReadNegativeStrand) // only featurize positive strands.
      )                                           // This will be changed when we have channels

    val dnaseRDD = mergeDnase(sc, rdd, sd, cells, dnase)

    dnaseRDD.map(f => {
      val kx = (kernelApprox(oneHotEncodeDnase(f)))
      BaseFeature(f.labeledWindow, kx)
    })
  }


  /**
   * One hot encodes sequences with dnase data
 *
   * @param f feature with sequence and featurized dnase
   * @return Densevector of sequence and dnase mushed together
   */
  private[pipelines] def oneHotEncodeDnase(f: BaseFeature): DenseVector[Double] = {

      // form seq of int from bases and join with dnase
      val intString: Seq[(Int, Double)] = f.labeledWindow.win.sequence.map(r => Dataset.alphabet.get(r).getOrElse(-1))
        .zip(f.features.toArray)

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
   * Merge dnase cuts windows
 *
   * @param sc SparkContext
   * @param cells cells to get DNASE for
   * @return
   */
  def mergeDnase(sc: SparkContext,
                 rdd: RDD[LabeledWindow],
                 sd: SequenceDictionary,
                 cells: Array[CellTypes.Value],
                 dnase: AlignmentRecordRDD,
                 subselectNegatives: Boolean = true): RDD[BaseFeature] = {

    // return cuts into vectors of counts
    VectorizedDnase.featurize(sc, rdd, dnase, sd, subselectNegatives, false, None, false)
      .map(r => BaseFeature(r.labeledWindow, r.features.slice(0, Dataset.windowSize))) // slice off just positives

  }

  /**
   * Saves predictions and labeled windows as a FeatureRDD.
 *
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
