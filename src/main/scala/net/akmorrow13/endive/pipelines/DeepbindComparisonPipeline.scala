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
import nodes.learning.{BlockLinearMapper, LBFGSwithL2, BlockLeastSquaresEstimator}
import nodes.util.{Cacher, MaxClassifier, ClassLabelIndicatorsFromIntLabels}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.bdgenomics.adam.rdd.ADAMContext._
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import java.io.{PrintWriter, File}



object DeepbindComparisonPipeline extends Serializable with Logging {

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
    val dnaseSize = 10
    val seqSize = 101
    val alphabetSize = Dataset.alphabet.size
    val testLoc = conf.getSaveValPredictions
    val positivesLoc = conf.windowLoc + ".positives.fasta"
    val negativesLoc = conf.windowLoc + ".negatives.fasta"

    var (train, eval) = {

      val trainPositives = sc.loadFasta(positivesLoc, seqSize).rdd.map(r => (r, 1))
      val trainNegatives = sc.loadFasta(negativesLoc, seqSize).rdd.map(r => (r, 0))

      val tf = TranscriptionFactors.Any
      val cell = CellTypes.Any
      val region = ReferenceRegion("chr",1,101)
      val train = trainPositives.union(trainNegatives)
        .map(r => {
          val win = Window(tf, cell, region,r._1.getFragmentSequence)
          LabeledWindow(win, r._2)
        })

      val eval = sc.loadFasta(testLoc, seqSize).rdd.zipWithIndex()
            .map(r => {
              if (r._2 < 500) (r._1, 1)
              else (r._1, 0)
            })
        .map(r => {
          val win = Window(tf, cell, region,r._1.getFragmentSequence)
          LabeledWindow(win, r._2)
        })

      assert(eval.count == 1000)

      (train.repartition(200), eval.repartition(3))
    }


    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val W_sequence = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian)

    // generate approximation features
    val (trainFeatures, evalFeatures) =
       (DnaseKernelPipeline.featurize(train, Array(W_sequence), Array(kmerSize)).map(_.features),
         DnaseKernelPipeline.featurize(eval, Array(W_sequence), Array(kmerSize)).map(_.features))


    val labelExtractor = ClassLabelIndicatorsFromIntLabels(2) andThen
      new Cacher[DenseVector[Double]]

    val trainLabels = labelExtractor(train.map(_.label)).get
    val evalLabels = labelExtractor(eval.map(_.label)).get

    trainFeatures.map(x => x.toArray.mkString(",")).zip(trainLabels).map(x => s"${x._1}|${x._2}").saveAsTextFile(s"${conf.featurizedOutput}_${approxDim}_train")
    evalFeatures.map(x => x.toArray.mkString(",")).zip(evalLabels).map(x => s"${x._1}|${x._2}").saveAsTextFile(s"${conf.featurizedOutput}_${approxDim}_eval")

    val model = new BlockLeastSquaresEstimator(approxDim, 2, conf.lambda).fit(trainFeatures, trainLabels)

    val allYTrain = model(trainFeatures)
    val allYEval = model(evalFeatures)

    val zippedTrainResults: RDD[(Double, Double)] = allYTrain.zip(trainLabels).map(r => (r._1(0), r._2(0)))
    val zippedEvalResults: RDD[(Double, Double)] = allYEval.zip(evalLabels).map(r => (r._1(0), r._2(0)))


    // save results as local file
    zippedTrainResults.map(r => s"${r._1},${r._2}").repartition(1).saveAsTextFile(s"/icml/deepbind_tests/results/${conf.windowLoc}_trainScores.csv")
    zippedEvalResults.map(r => s"${r._1},${r._2}").repartition(1).saveAsTextFile(s"/icml/deepbind_tests/results/${testLoc}_evalScores.csv")

    val evalTrain= new BinaryClassificationMetrics(zippedTrainResults)
    Metrics.printMetrics(evalTrain, Some(s"Eval metrics for ${testLoc}"))

    val evalEval = new BinaryClassificationMetrics(zippedEvalResults)
    Metrics.printMetrics(evalEval, Some(s"Eval metrics for ${testLoc}"))


  }

}
