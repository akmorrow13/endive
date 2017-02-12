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
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import com.github.fommil.netlib.BLAS
import nodes.learning.{BlockLeastSquaresEstimator}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import java.io.{PrintWriter, File}
import org.apache.spark.mllib.linalg.Vectors



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
    val kmerSize = Array(6, 8, 12)
    val approxDim = conf.approxDim
    val dnaseSize = 100
    val seqSize = 200
    val alphabetSize = Dataset.alphabet.size

    // generate headers
    val headers = sc.textFile(conf.deepSeaDataPath + "headers.csv").first().split(",").map(r => r.split("|"))
    val headerTfs: Array[String] = headers.map(r => r(1))
    println("headerTfs")
    headerTfs.foreach(println)

    val index = sc.textFile(conf.deepSeaDataPath + "headers.csv").first().split(",").indexOf(conf.getSample)

    var (train, eval) = {
      val train = LabeledWindowLoader(conf.getWindowLoc, sc).setName("_All data")
      val eval = LabeledWindowLoader(conf.getWindowLoc, sc).setName("_eval")
      (reduceLabels(headerTfs, train), reduceLabels(headerTfs, eval))
    }

    // Slice windows for 200 bp range
    train = train.repartition(700).cache().map(r => {
      val mid = r.win.getRegion.length /2 + r.win.getRegion.start
      LabeledWindow(r.win.slice(mid-100,mid+100), r.labels)
    })
    train.count()

    eval = eval.repartition(2).cache().map(r => {
      val mid = r.win.getRegion.length /2 + r.win.getRegion.start
      LabeledWindow(r.win.slice(mid-100,mid+100), r.labels)
    })
    eval.count

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val W_sequence = DenseMatrix.rand(approxDim, kmerSize(0) * alphabetSize, gaussian)

    // generate approximation features
    val (trainFeatures, evalFeatures) =
     if (conf.useDnase) {
       (DnaseKernelPipeline.featurizeWithDnase(sc, train, W_sequence, kmerSize, dnaseSize, approxDim).map(_.features),
         DnaseKernelPipeline.featurizeWithDnase(sc, eval, W_sequence, kmerSize, dnaseSize, approxDim).map(_.features))
     } else {
       (DnaseKernelPipeline.featurize(train, W_sequence, kmerSize).map(_.features),
         DnaseKernelPipeline.featurize(eval, W_sequence, kmerSize).map(_.features))
     }

    val trainLabels = train.map(_.labels.map(_.toDouble)).map(DenseVector(_))
    val evalLabels = eval.map(_.labels.map(_.toDouble)).map(DenseVector(_))

    val model = new BlockLeastSquaresEstimator(approxDim, 1, conf.lambda).fit(trainFeatures, trainLabels)

    val allYTrain = model(trainFeatures)
    val allYEval = model(evalFeatures)

    val tfs: Array[TranscriptionFactors.Value] = conf.tfs.split(',').map(r => TranscriptionFactors.withName(r))

    val spots = headers.zipWithIndex.filter(r => !tfs.map(_.toString).filter(tf => r._1.contains(tf)).isEmpty)
    println("selected tfs")
    spots.foreach(println)

    // get max scores. Used for normalization
   val maxVector = DenseVector(spots.map(i => {
     allYTrain.map(r => r(i._2)).max
   }))

  // score motifs
  val motifs =
    if (conf.motifDBPath != null && conf.getModelTest != null) {
      Some(DnaseKernelPipeline.scoreMotifs(sc, tfs, conf.motifDBPath, conf.getModelTest,
        W_sequence, model, maxVector, kmerSize(0), seqSize))
    } else {
      None
    }
    println(motifs)

    // get metrics
    DnaseKernelPipeline.printAllMetrics(headerTfs, tfs.map(_.toString), allYTrain.zip(trainLabels), allYEval.zip(evalLabels), motifs)

    val valResults:String = evalLabels.zip(allYEval).map(x => s"${x._1.toArray.mkString(",")},${x._2.toArray.mkString(",")}").collect().mkString("\n")
    val trainResults:String = trainLabels.zip(allYTrain).map(x => s"${x._1.toArray.mkString(",")},${x._2.toArray.mkString(",")}").collect().mkString("\n")
    val pwTrain = new PrintWriter(new File("/tmp/deepsea_train_results" ))
    val scoreHeaders = headerTfs.map(x => x + "_label").mkString(",")
    val truthHeaders = headerTfs.map(x => x + "_score").mkString(",")
    println("HEADER SIZE " + truthHeaders.split(",").size)
    println("LABEL SIZE " + trainLabels.first.size)
    val resultsHeaders = scoreHeaders + "," + truthHeaders + "\n"
    pwTrain.write(resultsHeaders)
    pwTrain.write(trainResults)
    pwTrain.close

    var pwVal = new PrintWriter(new File("/tmp/deepsea_val_results" ))
    pwVal.write(resultsHeaders)
    pwVal.write(valResults)
    pwVal.close

  }


  def reduceLabels(headerTfs: Array[String], rdd: RDD[LabeledWindow]): RDD[LabeledWindow] = {

      rdd.map(r => {
        // map of (tf, bound in any cell type) tuple
        val x: Map[String, Boolean] = headerTfs.zip(r.labels)
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
            if (x.get(r).get) 1
            else 0
          } catch {
            case e: Exception => throw new Exception(s"${r} not in ${x}")
          }
        })
        LabeledWindow(r.win, newLabels)
      })
  }
}
