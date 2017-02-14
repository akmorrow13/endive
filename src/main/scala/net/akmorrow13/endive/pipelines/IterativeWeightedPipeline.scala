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
import net.akmorrow13.endive.featurizers.Kmer
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import com.github.fommil.netlib.BLAS
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.{BlockWeightedLeastSquaresEstimator, BlockLeastSquaresEstimator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.bdgenomics.adam.rdd.contig._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.formats.avro.{NucleotideContigFragment, Fragment}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import net.akmorrow13.endive.metrics.Metrics



object IterativeWeightedPipeline extends Serializable with Logging {

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
    val alphabetSize = Dataset.alphabet.size
    val approxDim = conf.approxDim
    val kmerSize = conf.kmerLength
    val featuresName = conf.featuresOutput
    val trainFeaturesName = featuresName + s"_${approxDim}_train"
    val valFeaturesName = featuresName + s"_${approxDim}_eval"

    // generate headers
    val headers = sc.textFile(conf.deepSeaDataPath + "headers.csv").first().split(",")
    val headerTfs: Array[String] = headers.map(r => r.split('|')).map(r => r(1))

    val indexTf = headers.zipWithIndex.filter(r => r._1.contains(conf.tfs)).head
    println(indexTf)

    val trainFiles = sc.textFile(trainFeaturesName)
    val evalFiles = sc.textFile(valFeaturesName)

    val (train, eval) = {

      val train = trainFiles.map(x => DenseVector(x.split("\\|").map(r => r.split(","))))
        .map(r => (DenseVector(r(0).map(_.toDouble)), DenseVector(r(1).map(_.toDouble))))

      val eval = evalFiles.map(x => DenseVector(x.split("\\|").map(r => r.split(","))))
        .map(r => (DenseVector(r(0).map(_.toDouble)), DenseVector(r(1).map(_.toDouble))))

      (train, eval)
    }

    // initial ROC and ucPRC scores
    var roc = 0.8
    var prc = 0.8

    var negSample = 0.005
    // start with equal positives and negatives
    var sampledTrainFeatures = {
      val negatives = train.filter(_._2(indexTf._2) == 0).sample(false, negSample)
      val positives = train.filter(_._2(indexTf._2) > 0)
      negatives.union(positives)
    }

    var negCount =  sampledTrainFeatures.filter(_._2(indexTf._2) == 0).count
    var posCount =  sampledTrainFeatures.filter(_._2(indexTf._2) == 0).count

    var mixtureWeight = negCount.toDouble / posCount.toDouble
    println(s"mixture weight ${mixtureWeight}")

    while(true) {
      println(s"running for sample: ${posCount} positives and ${negCount} negatives. Current Metrics : ROC ${roc}, auPRC: ${prc}")

      var metrics = runModel(sampledTrainFeatures, eval, conf.lambda, indexTf._2, mixtureWeight)

      // while scores are low keep raising weight. once metrics are high enough, reset them and add in more negatives
      while (metrics._1 < roc && metrics._2 < prc) {
        mixtureWeight = mixtureWeight * 2
        println(s"raising mixture weight to ${mixtureWeight}")
        metrics = runModel(train, eval, conf.lambda, indexTf._2, mixtureWeight)
      }

      // found a good weight. continue
      roc = metrics._1
      prc = metrics._2

      negSample += 0.1
      sampledTrainFeatures = {
        val negatives = train.filter(_._2(indexTf._2) == 0).sample(false, negSample)
        val positives = train.filter(_._2(indexTf._2) > 0)
        negatives.union(positives)
      }

      negCount =  sampledTrainFeatures.filter(_._2(indexTf._2) == 0).count
      posCount =  sampledTrainFeatures.filter(_._2(indexTf._2) == 0).count
    }

  }

  def runModel(train: RDD[(DenseVector[Double],DenseVector[Double])],
               eval: RDD[(DenseVector[Double],DenseVector[Double])],
               lambda: Double, idx: Int, mixtureWeight: Double): Tuple2[Double, Double] = {

    val model = new BlockWeightedLeastSquaresEstimator(4096, 1, lambda, mixtureWeight).fit(train.map(_._1), train.map(_._2))
    val allYTrain = model(train.map(_._1))
    val zippedTrainResults: RDD[(Double, Double)] = allYTrain.zip(train.map(_._2)).map(r => (r._1(idx), r._2(idx)))

    // get metrics for train
    val trainEval = new BinaryClassificationMetrics(zippedTrainResults)
    val trainMetrics = Metrics.getMetrics(trainEval)
    println(s"Train: weight: ${mixtureWeight}, ROC: ${trainMetrics._1}, auPRC: ${trainMetrics._2}")

    // get metrics for eval
    val allYEval = model(eval.map(_._1))
    val zippedEvalResults = allYEval.zip(eval.map(_._2)).map(r => (r._1(idx), r._2(idx)))
    val evalEval = new BinaryClassificationMetrics(zippedEvalResults)
    val evalMetrics = Metrics.getMetrics(evalEval)
    println(s"Eval: weight: ${mixtureWeight}, ROC: ${evalMetrics._1}, auPRC: ${evalMetrics._2}")

    return trainMetrics
  }

  /**
   * Merges all labels for one transcription factor
   * @param headerTfs
   * @param rdd
   * @return
   */
  def reduceLabels(headerTfs: Array[String], rdd: RDD[DenseVector[Double]]): RDD[DenseVector[Double]] = {

      rdd.map(r => {
        // map of (tf, bound in any cell type) tuple
        val x: Map[String, Boolean] = headerTfs.zip(r.toArray)
          .groupBy(_._1)
          .mapValues(r => {
              if (r.map(_._2).sum > 0)
                true
              else false
          })
        // generate new labels
        val newLabels =
        headerTfs.map(r => {
          try {
            if (x.get(r).get) 1.0
            else 0
          } catch {
            case e: Exception => throw new Exception(s"${r} not in ${x}")
          }
        })
        DenseVector(newLabels)
      })
  }
}
