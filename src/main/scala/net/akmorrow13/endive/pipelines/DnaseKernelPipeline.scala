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
import net.jafama.FastMath
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



object DnaseKernelPipeline extends Serializable with Logging {

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

    var featuresOutput = conf.featuresOutput

    if (dataPath == null) {
      println("Error: no data or reference path")
      sys.exit(-1)
    }

    val train = MergeLabelsAndSequences.joinDnaseAndSequence(sc, s"${conf.getSequenceLoc}deepsea_train_reg_seq",
      s"${conf.getDnaseLoc}train/${conf.cellTypes}", s"${conf.getAggregatedSequenceOutput}train_${conf.cellTypes}", 200)
      .setName("_All data")

    val eval = MergeLabelsAndSequences.joinDnaseAndSequence(sc, s"${conf.getSequenceLoc}deepsea_eval_reg_seq",
      s"${conf.getDnaseLoc}eval/${conf.cellTypes}", s"${conf.getAggregatedSequenceOutput}eval_${conf.cellTypes}", 2)
      .setName("_All data")

    sys.exit(0)


    //    // load data for a specific transcription factor
//    var train: RDD[LabeledWindow] =
//      LabeledWindowLoader(dataPath, sc).setName("_All data")

    train.repartition(700).cache()
    train.count()

//    // normalize dnase
//    val dnaseMaxPos = train.map(r => r.win.getDnase.max).max
//    train = train.map(r => {
//      val win = r.win.setDnase(r.win.getDnase / dnaseMaxPos)
//      LabeledWindow(win, r.label)
//    })
//    println(s"max for dnase went from ${dnaseMaxPos} to ${train.map(r => r.win.getDnase.max).max}")

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val W_sequence = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian)

    // generate approximation features
    val (trainFeatures, evalFeatures) =
     if (conf.useDnase) {
       (featurizeWithWaveletDnase(sc, train, W_sequence, kmerSize, dnaseSize, approxDim),
         featurizeWithWaveletDnase(sc, eval, W_sequence, kmerSize, dnaseSize, approxDim))
     } else {
       (featurizeWithDnase(sc, train, W_sequence, kmerSize, dnaseSize, approxDim),
         featurizeWithDnase(sc, eval, W_sequence, kmerSize, dnaseSize, approxDim))
     }

    val trainLabels = trainFeatures.map(_.labeledWindow.labels.map(_.toDouble)).map(DenseVector(_))
    val evalLabels = evalFeatures.map(_.labeledWindow.labels.map(_.toDouble)).map(DenseVector(_))

    val model = new BlockLeastSquaresEstimator(approxDim, 1, conf.lambda).fit(trainFeatures.map(_.features), trainLabels)






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

    val kernelApprox = new KernelApproximator(W, FastMath.cos, kmerSize, Dataset.alphabet.size, fastfood=false)

    matrix.map(f => {
      val kx = (kernelApprox({
        KernelApproximator.stringToVector(f.win.getSequence)
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
                         approxDim: Int,
			 gamma: Double = 1.0): RDD[FeaturizedLabeledWindow] = {

    val kernelApprox_seq = new KernelApproximator(W_sequence, FastMath.cos, kmerSize, Dataset.alphabet.size, fastfood=false)

    val gaussian = new Gaussian(0, 1)

    // generate kernel approximators for all different variations of dnase length
    val approximators = Array(100, 50, 25, 12, 6)
      .map(r => {
        val W_pos = DenseMatrix.rand(approxDim, r, gaussian)
	val W_neg = DenseMatrix.rand(approxDim, r, gaussian)
        (new KernelApproximator(W_pos, FastMath.cos, r, 1, fastfood=false, gamma=gamma), new KernelApproximator(W_neg, FastMath.cos, r, 1, fastfood=false, gamma=gamma))
      })


    rdd.map(f => {

      val k_seq = kernelApprox_seq(KernelApproximator.stringToVector(f.win.getSequence))

      //iteratively compute and multiply all dnase for positive strands
      val k_dnase_pos = approximators.map(ap => {
        ap._1(f.win.getDnase.slice(0, Dataset.windowSize))
      }).reduce(_ :* _)

      //iteratively compute and multiply all dnase for negative strands
      val k_dnase_neg = approximators.map(ap => {
        ap._2(f.win.getDnase.slice(Dataset.windowSize, f.win.getDnase.length))
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
   * @param dnaseSize size of dnase
   * @return
   */
  def featurizeWithDnase(sc: SparkContext,
                         rdd: RDD[LabeledWindow],
                         W_sequence: DenseMatrix[Double],
                         kmerSize: Int,
                         dnaseSize: Int,
                         approxDim: Int,
			 gamma: Double = 1.0): RDD[FeaturizedLabeledWindow] = {

    val kernelApprox_seq = new KernelApproximator(W_sequence, FastMath.cos, kmerSize, Dataset.alphabet.size)

    val gaussian = new Gaussian(0, 1)
    val W_dnase_pos = DenseMatrix.rand(approxDim, dnaseSize, gaussian)
    val W_dnase_neg = DenseMatrix.rand(approxDim, dnaseSize, gaussian)

    val kernelApprox_dnase_pos = new KernelApproximator(W_dnase_pos, FastMath.cos, dnaseSize, 1, fastfood=false, gamma=gamma)
    val kernelApprox_dnase_neg = new KernelApproximator(W_dnase_neg, FastMath.cos, dnaseSize, 1, fastfood=false, gamma=gamma)

    rdd.map(f => {
      val k_seq = kernelApprox_seq(KernelApproximator.stringToVector(f.win.getSequence))
      val k_dnase_pos = kernelApprox_dnase_pos(f.win.getDnase.slice(0, Dataset.windowSize))
      val k_dnase_neg = kernelApprox_dnase_neg(f.win.getDnase.slice(Dataset.windowSize, f.win.getDnase.length))

      FeaturizedLabeledWindow(f, DenseVector.vertcat(k_seq, k_dnase_pos, k_dnase_neg))
    })
  }

  /**
   * One hot encodes sequences with dnase data
   * @param f feature with sequence and featurized dnase
   * @return Densevector of sequence and dnase mushed together
   */
  private[pipelines] def oneHotEncodeDnase(f: LabeledWindow): DenseVector[Double] = {

      // form seq of int from bases and join with dnase
      val intString: Seq[(Int, Double)] = f.win.getSequence.map(r => Dataset.alphabet.get(r).getOrElse(-1))
        .zip(f.win.getDnase.slice(0, Dataset.windowSize).toArray) // slice off just positives

      val seqString = intString.map { r =>
        val out = DenseVector.zeros[Double](Dataset.alphabet.size)
        if (r._1 != -1) {
          out(r._1) = 1 + r._2
        }
        out
      }
      DenseVector.vertcat(seqString: _*)
  }

}
