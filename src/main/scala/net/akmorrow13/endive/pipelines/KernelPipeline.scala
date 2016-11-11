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
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.processing.Dataset.{ CellTypes}
import net.akmorrow13.endive.utils._
import net.jafama.FastMath
import nodes.learning._
import nodes.util._

import com.github.fommil.netlib.BLAS
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{ SequenceDictionary }
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import java.io.{File, BufferedWriter, FileWriter}


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
    val approxDim = 4000
    val alphabetSize = Dataset.alphabet.size

    val dataPath = conf.aggregatedSequenceOutput
    val referencePath = conf.reference

    if (dataPath == null) {
      println("Error: no data or reference path")
      sys.exit(-1)
    }

    // load data for a specific transcription factor
    val allData: RDD[LabeledWindow] =
      LabeledWindowLoader(dataPath, sc).setName("_All data").cache()

    // extract tfs and cells for this label file
    val tfs = allData.map(_.win.getTf).distinct.collect()
    val cells = allData.map(_.win.getCellType).distinct.collect()

    println(s"running on TFs from ${dataPath}:")
    tfs.foreach(println)
    println(s"running on cells from ${dataPath}:")
    cells.foreach(println)

    // divvy up into train and test
    val (train, test) = {
      val first = allData.first
      val referenceName = first.win.getRegion.referenceName
      val cell = first.win.cellType

      // hold out a (chromosome, celltype) pair for test
      val train = allData.filter(r => (r.win.getRegion.referenceName != referenceName && r.win.cellType != cell))
      val test = allData.filter(r => (r.win.getRegion.referenceName == referenceName && r.win.cellType == cell))

      (train, test)
    }

    // create sequence dictionary, used to save files
    val sd: SequenceDictionary =
      if (referencePath != null) {
        val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
        reference.sequences
      } else null


    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val W = 0.1 * DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian)

    // generate approximation features
    val trainApprox = featurize(train, W, kmerSize)
    val testApprox = featurize(test, W, kmerSize)

    val predictor = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 10, regParam=0.01)
      .fit(trainApprox.map(_.features), trainApprox.map(_.labeledWindow.label))

    val trainPredictions = predictor(trainApprox.map(_.features))
    val evalTrain = new BinaryClassificationMetrics(trainPredictions.zip(trainApprox.map(_.labeledWindow.label.toDouble)))
    println("Train Results: \n ")
    Metrics.printMetrics(evalTrain)

    val testPredictions = predictor(testApprox.map(_.features))
    val evalTest = new BinaryClassificationMetrics(trainPredictions.zip(testApprox.map(_.labeledWindow.label.toDouble)))
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
        val BASEPAIRMAP = Map('N' -> -1, 'A' -> 0, 'T' -> 1, 'C' -> 2, 'G' -> 3)
        val sequenceVectorizer = ClassLabelIndicatorsFromIntLabels(4)

        val intString: Seq[Int] = f.win.sequence.map(BASEPAIRMAP(_))
        val seqString = intString.map { bp =>
          val out = DenseVector.zeros[Double](4)
          if (bp != -1) {
            out(bp) = 1
          }
          out
        }
        DenseVector.vertcat(seqString: _*)
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

    // TODO does this give you all datapoints?
    val dnaseRDD = mergeDnase(sc, rdd, sd, cells, dnasePath)


    dnaseRDD.map(f => {
      val kx = (kernelApprox({
        val BASEPAIRMAP = Map('N' -> -1, 'A' -> 0, 'T' -> 1, 'C' -> 2, 'G' -> 3)

        // form seq of int from bases and join with dnase
        val intString: Seq[(Int, Double)] = f._1.win.sequence.map(BASEPAIRMAP(_))
          .zip(f._2.toArray)

        val seqString = intString.map { r =>
          val out = DenseVector.zeros[Double](4)
          if (r._1 != -1) {
            out(r._1) = 1 + r._2
          }
          out
        }
        DenseVector.vertcat(seqString: _*)
      }))
      BaseFeature(f._1, kx)
    })

  }

    /**
     * Merge dnase cuts windows
     * @param sc SparkContext
     * @param cells cells to get DNASE for
     * @return
     */
    def mergeDnase(sc: SparkContext,
                   rdd: RDD[LabeledWindow],
                   sd: SequenceDictionary,
                   cells: Array[CellTypes.Value],
                   dnasePath: String): RDD[(LabeledWindow, DenseVector[Double])] = {

      // load cuts. TODO: are these cell type specific?
      val cuts: RDD[Cut] = Preprocess.loadCuts(sc, dnasePath, cells)
        .filter(!_.negativeStrand) // only featurize positive strands. This will be changed when we have channels

      val dnase = new Dnase(Dataset.windowSize, Dataset.stride, sc, cuts)
      val coverage = dnase.merge(sd).cache()
      coverage.count

      // return cuts into vectors of counts
      VectorizedDnase.featurize(sc, rdd, coverage, sd, true, false, None, false)
        .map(r => (r.labeledWindow, r.features))

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