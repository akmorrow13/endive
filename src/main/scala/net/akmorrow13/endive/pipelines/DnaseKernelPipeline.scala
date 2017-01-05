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
import nodes.learning.{ BlockLeastSquaresEstimator }
import nodes.util.{Cacher, MaxClassifier, ClassLabelIndicatorsFromIntLabels}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import java.io.File
import net.jafama.FastMath



object DnaseKernelPipeline  extends Serializable with Logging {

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
    val approxDim = conf.approxDim
    val dnaseSize = 100
    val alphabetSize = Dataset.alphabet.size

    val dataPath = conf.aggregatedSequenceOutput
    val dnasePath = conf.dnaseNarrow
    val referencePath = conf.reference

    if (dataPath == null || referencePath == null) {
      println("Error: no data or reference path")
      sys.exit(-1)
    }

    // create sequence dictionary, used to save files
    val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
    val sd: SequenceDictionary = reference.sequences


    // load data for a specific transcription factor
    val allData: RDD[LabeledWindow] =
      LabeledWindowLoader(dataPath, sc).setName("_All data")
        .filter(_.label >= 0)
        .cache()

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val W_sequence = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian)
    val W_dnase = DenseMatrix.rand(approxDim, dnaseSize * 1, gaussian)

    // generate approximation features
    val allFeaturized = featurizeWithWaveletDnase(sc, allData, W_sequence, kmerSize, dnaseSize, approxDim)

    allFeaturized.map(_.toString).saveAsTextFile(conf.featuresOutput)

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
                            kmerSize: Int): RDD[FeaturizedLabeledWindow] = {

    val kernelApprox = new KernelApproximator(W, Math.cos, kmerSize, Dataset.alphabet.size)

    matrix.map(f => {
      val kx = (kernelApprox({
        KernelApproximator.stringToVector(f.win.sequence)
      }))
      FeaturizedLabeledWindow(f, kx)
    })

  }

  /**
   * Takes a region of the genome and one hot enodes sequences (eg A = 0001). If DNASE is enabled positive strands
   * are appended to the one hot sequence encoding. For example, if positive 1 has A with DNASE count of 3 positive
   * strands, the encoding is 0003.
   *
   * @param sc: Spark Context
   * @param rdd: data matrix with sequences
   * @return
   */
  def featurizeWithWaveletDnase(sc: SparkContext,
                         rdd: RDD[LabeledWindow],
                         W_sequence: DenseMatrix[Double],
                         kmerSize: Int,
   			 dnaseSize: Int,
                         approxDim: Int): RDD[FeaturizedLabeledWindow] = {

    val kernelApprox_seq = new KernelApproximator(W_sequence, Math.cos, kmerSize, Dataset.alphabet.size)

    val gaussian = new Gaussian(0, 1)

    // generate kernel approximators for all different variations of dnase length
    val approximators = Array(100, 50, 25, 12, 6)
      .map(r => {
        val W = DenseMatrix.rand(approxDim, r, gaussian)
        new KernelApproximator(W, Math.cos, r, 1)
      })


    rdd.map(f => {

      val k_seq = kernelApprox_seq(KernelApproximator.stringToVector(f.win.sequence))

      //iteratively compute and multiply all dnase for positive strands
      val k_dnase_pos = approximators.map(ap => {
        ap(f.win.dnase.slice(0, Dataset.windowSize))
      }).reduce(_ :* _)

      //iteratively compute and multiply all dnase for negative strands
      val k_dnase_neg = approximators.map(ap => {
        ap(f.win.dnase.slice(Dataset.windowSize, f.win.dnase.length))
      }).reduce(_ :* _)

      FeaturizedLabeledWindow(f, DenseVector.vertcat(k_seq, k_dnase_pos, k_dnase_neg))
    })

  }

  /**
   * Takes a region of the genome and one hot enodes sequences (eg A = 0001). If DNASE is enabled positive strands
   * are appended to the one hot sequence encoding. For example, if positive 1 has A with DNASE count of 3 positive
   * strands, the encoding is 0003.
   *
   * @param sc: Spark Context
   * @param rdd: data matrix with sequences
   * @param kmerSize size of kmers
   * @param W_sequence random matrix for sequence
   * @param W_dnase random matrix for dnase
   * @param dnaseSize size of dnase
   * @return
   */
  def featurizeWithDnase(sc: SparkContext,
                         rdd: RDD[LabeledWindow],
                         W_sequence: DenseMatrix[Double],
                         kmerSize: Int,
                         W_dnase: Option[DenseMatrix[Double]] = None,
                         dnaseSize: Option[Int] = None): RDD[FeaturizedLabeledWindow] = {

    val kernelApprox_seq = new KernelApproximator(W_sequence, FastMath.cos, kmerSize, Dataset.alphabet.size)

    if (W_dnase.isDefined && dnaseSize.isDefined) {
      val kernelApprox_dnase = new KernelApproximator(W_dnase.get, Math.cos, dnaseSize.get, 1)
      println(s"seqSize, ${rdd.first.win.dnase.slice(0, Dataset.windowSize).size}")
      rdd.map(f => {
        val k_seq = kernelApprox_seq(KernelApproximator.stringToVector(f.win.sequence))
        val k_dnase_pos = kernelApprox_dnase(f.win.dnase.slice(0, Dataset.windowSize))
        val k_dnase_neg = kernelApprox_dnase(f.win.dnase.slice(Dataset.windowSize, f.win.dnase.length))

        FeaturizedLabeledWindow(f, DenseVector.vertcat(k_seq, k_dnase_pos, k_dnase_neg))
      })
    } else {
      rdd.map(f => {
        val kx = kernelApprox_seq(oneHotEncodeDnase(f))
        FeaturizedLabeledWindow(f, kx)
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

    val featureRDD = new FeatureRDD(features, sd)
    featureRDD.save(path, false)
  }
}
