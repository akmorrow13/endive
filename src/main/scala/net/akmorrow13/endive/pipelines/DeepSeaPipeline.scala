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

import java.io._
import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.{Chromosomes, CellTypes, TranscriptionFactors}
import net.akmorrow13.endive.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.bdgenomics.adam.rdd.feature.CoverageRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{TwoBitFile}
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.BlockLeastSquaresEstimator
import net.jafama.FastMath
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics

object DeepSeaPipeline extends Serializable  {

  /**
   * A very basic dataset creation pipeline for sequence data that *doesn't* featurize the data
   * but creates a csv of (Window, Label)
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
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.INFO)
      val conf = new SparkConf().setAppName(configtext)
      conf.setIfMissing("spark.master", "local[4]")
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {
    val headers = sc.textFile(conf.deepSeaDataPath + "headers").collect()(0).split(",")
    val trainFiles = sc.textFile(conf.deepSeaDataPath + "deepsea_train").map(x => x.split(","))
    val evalFiles = sc.textFile(conf.deepSeaDataPath + "deepsea_eval").map(x => x.split(","))
    println(conf.deepSeaTfs.mkString(","))
    val tfIndices = headers.zipWithIndex.filter(x => conf.deepSeaTfs.map(y => x._1 contains y).contains(true)).map(x => x._2)
    println(tfIndices.mkString(","))
    println(headers.size)


    val labelHeaders = headers.zipWithIndex.filter(x => conf.deepSeaTfs.map(y => x._1 contains y).contains(true)).map(x => x._1)

    val tfIndicesB = sc.broadcast(tfIndices)

    val trainSeqs = trainFiles.map(x => x(0).substring(400,600)).map(KernelApproximator.stringToVector(_))
    val evalSeqs = evalFiles.map(x => x(0).substring(400,600)).map(KernelApproximator.stringToVector(_))

    val trainLabels = trainFiles.map(x => tfIndicesB.value.map(y => x(y).toDouble)).map(DenseVector(_)).setName("trainLabels").cache()
    val evalLabels = evalFiles.map(x => tfIndicesB.value.map(y => x(y).toDouble)).map(DenseVector(_)).setName("evalLabels").cache()

    println("TRAIN LABELS " + trainLabels.count())
    println("TRAIN LABELS SIZE " + trainLabels.first.size)

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(conf.seed)))
    val gaussian = new Gaussian(0, 1) 
    val seqSize = trainSeqs.first.size
    val kmerSize = conf.kmerLength
    val alphabetSize = conf.alphabetSize
    val approxDim = conf.approxDim
    // generate random matrix
    val W:DenseMatrix[Double] = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian) * conf.gamma
    val uniform = new Uniform(0, 2*math.Pi)
    val phase = DenseVector.rand(approxDim, uniform)
    val kernelApprox = new KernelApproximator(W, FastMath.cos, ngramSize = kmerSize, alphabetSize=4, offset=Some(phase), fastfood=false, seed=conf.seed, gamma=conf.gamma, seqSize=seqSize/alphabetSize)
    val trainFeatures = kernelApprox(trainSeqs).setName("trainFeatures").cache()
    val evalFeatures = kernelApprox(evalSeqs).setName("evalFeatures").cache()

    val featuresName = s"${seqSize}_${kmerSize}_${approxDim}_${conf.gamma}"
    val trainFeaturesName = featuresName + "_train.features"
    val valFeaturesName = featuresName + "_val.features"

    trainFeatures.map(x => x.toArray.mkString(",")).zip(trainLabels).map(x => s"${x._1}|${x._2.toArray.mkString(",")}").saveAsTextFile(trainFeaturesName)
    evalFeatures.map(x => x.toArray.mkString(",")).zip(evalLabels).map(x => s"${x._1}|${x._2.toArray.mkString(",")}").saveAsTextFile(valFeaturesName)

    println(s"FEATURIZING WITH KMERSIZE=${kmerSize}, gamma=${conf.gamma}")
    println(s"NUMBER OF TRAINING POINTS ${trainFeatures.count()}")
    println("Solving least squares")
    val model = new BlockLeastSquaresEstimator(min(1024, approxDim), 1, conf.lambda).fit(trainFeatures, trainLabels)
    val allYTrain = model(trainFeatures)
    val allYEval = model(evalFeatures)

    val valResults:String = evalLabels.zip(allYEval).map(x => s"${x._1.toArray.mkString(",")},${x._2.toArray.mkString(",")}").collect().mkString("\n")
    val trainResults:String = trainLabels.zip(allYTrain).map(x => s"${x._1.toArray.mkString(",")},${x._2.toArray.mkString(",")}").collect().mkString("\n")
    var pwTrain = new PrintWriter(new File("/tmp/deepsea_train_results" ))
    val scoreHeaders = labelHeaders.map(x => x + "_label").mkString(",")
    val truthHeaders = labelHeaders.map(x => x + "_score").mkString(",")
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
}
