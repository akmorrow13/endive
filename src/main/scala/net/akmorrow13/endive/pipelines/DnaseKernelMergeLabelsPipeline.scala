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

import net.akmorrow13.endive.featurizers.Kmer
import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import com.github.fommil.netlib.BLAS
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.{BlockLeastSquaresEstimator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import net.akmorrow13.endive.metrics.Metrics



object DnaseKernelMergeLabelsPipeline extends Serializable with Logging {

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
    val kmerSizes = Array(8)
//    val approxDim = conf.approxDim
    val dnaseSize = 10
    val seqSize = 200
    val alphabetSize = Dataset.alphabet.size

    // generate headers
    val headers = sc.textFile(conf.deepSeaDataPath + "headers.csv").first().split(",")
    val headerTfs: Array[String] = headers.map(r => r.split('|')).map(r => r(1))


    val indexTf = headers.zipWithIndex.filter(r => r._1.contains(conf.tfs)).head

    // load in train and eval as LabeledWindows
    val trainAll = LabeledWindowLoader(s"${conf.getWindowLoc}_train", sc).setName("_All data")
    .repartition(400).cache().map(r => {
      val mid = r.win.getRegion.length / 2 + r.win.getRegion.start
      val win =
        if (r.win.getDnase.length == 0) r.win.setDnase(DenseVector.zeros(r.win.getRegion.length.toInt))
        else r.win
      LabeledWindow(win.slice(mid - seqSize / 2, mid + seqSize / 2), r.labels)
    })
    trainAll.cache()
    trainAll.count()
    val positives = trainAll.map(_.labels(indexTf._2)).filter(_ > 0).count
    val negatives = trainAll.map(_.labels(indexTf._2)).filter(_ == 0).count
    val total = positives + negatives
    println(s"count: ${total}=${positives}(+)+${negatives}(-)")
    val negs = sc.parallelize(trainAll.filter(_.labels(indexTf._2) == 0).takeSample(false, positives.toInt))
    val train = negs.union(trainAll.filter(_.labels(indexTf._2) > 0))
    train.cache()
    train.count
    trainAll.unpersist()
//
//    eval = eval.repartition(2).cache().map(r => {
//      val mid = r.win.getRegion.length / 2 + r.win.getRegion.start
//      val win =
//        if (r.win.getDnase.length == 0) r.win.setDnase(DenseVector.zeros(r.win.getRegion.length.toInt))
//        else r.win
//      LabeledWindow(win.slice(mid - seqSize / 2, mid + seqSize / 2), r.labels)
//    })
//    eval.count()

    // base model
/*
    val baseFeatures = Kmer.extractKmers(train.map(_.win.getSequence), 6).map(r => DenseVector(r.toArray))
    val trainLabels = train.map(_.labels.map(_.toDouble)).map(DenseVector(_))

    val model = new BlockLeastSquaresEstimator(1024, 1, conf.lambda).fit(baseFeatures, trainLabels)
    val allYTrain = model(baseFeatures)

    val zippedTrainResults = allYTrain.zip(baseFeatures)

    // get metrics
    val evalEval = new BinaryClassificationMetrics(zippedTrainResults.map(r => (r._1(indexTf._2), r._2(indexTf._2))))
    Metrics.printMetrics(evalEval, Some(s"Eval,${indexTf._1},${indexTf._2}"))
    // end base model
*/
    val trainLabels = train.map(_.labels.map(_.toDouble)).map(DenseVector(_))
    
    val approxDims = Array(4096)
    for (approxDim <- approxDims) {

      implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
      val gaussian = new Gaussian(0, 1)

      // generate random matrix
      val W_sequences = kmerSizes.map(kmerSize => DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian))

      // generate approximation features
      val trainFeatures =
          DnaseKernelPipeline.featurize(train, W_sequences, kmerSizes).map(_.features)

      val model = new BlockLeastSquaresEstimator(approxDim, 1, conf.lambda).fit(trainFeatures, trainLabels)

      val allYTrain = model(trainFeatures)

      val zippedTrainResults = allYTrain.zip(trainLabels)

      // get metrics
        val evalEval = new BinaryClassificationMetrics(zippedTrainResults.map(r => (r._1(indexTf._2), r._2(indexTf._2))))
        Metrics.printMetrics(evalEval, Some(s"Eval,${indexTf._1},${indexTf._2}"))

    }

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
