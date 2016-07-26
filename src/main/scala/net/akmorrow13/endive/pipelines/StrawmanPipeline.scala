/**
 * Copyright 2015 Frank Austin Nothaft
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
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.Sequence
import net.akmorrow13.endive.utils._
import nodes.learning._
import nodes.nlp._
import nodes.stats.TermFrequency
import nodes.util.CommonSparseFeatures
import nodes.util.{Identity, Cacher, ClassLabelIndicatorsFromIntLabels, TopKClassifier, MaxClassifier, VectorCombiner}
import utils.{Image, MatrixUtils, Stats, ImageMetadata, LabeledImage, RowMajorArrayVectorizedImage, ChannelMajorArrayVectorizedImage}

import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging

object StrawmanPipeline extends Serializable with Logging {

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
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  def denseFeaturize(in: String): DenseVector[Double] = {
    /* Identity featurizer */

   val BASEPAIRMAP = Map('N'-> -1, 'A' -> 0, 'T' -> 1, 'C' -> 2, 'G' -> 3)
    val sequenceVectorizer = ClassLabelIndicatorsFromIntLabels(4)

    val intString:Seq[Int] = in.map(BASEPAIRMAP(_))
    val seqString = intString.map { bp =>
      if (bp == -1) {
        DenseVector.zeros[Double](4)
      } else {
        sequenceVectorizer(bp)
      }
    }
    DenseVector.vertcat(seqString:_*)
  }




  def run(sc: SparkContext, conf: EndiveConf) {
    println("STARTING STRAWMAN PIPELINE")
    val allData:RDD[LabeledWindow] = LabeledWindowLoader(conf.windowLoc, sc).filter(_.label >= 0)
    allData.count()

    println("Grouping Data for cross validation")
    val groupedData = allData.groupBy(lw => lw.win.chrosomeName).cache()
    groupedData.count()

    val foldsData = groupedData.map(x => (x._1.hashCode() % conf.folds, x._2))
    // featurize sequences to 8mers
    val labelVectorizer = ClassLabelIndicatorsFromIntLabels(2)

    for (i <- (0 until conf.folds)) {
      val train = foldsData.filter(x => x._1 != i).flatMap(x => x._2).cache()
      val test = foldsData.filter(x => x._1 == i).flatMap(x => x._2).cache()
      println(s"Fold ${i}, training points ${train.count()}, testing points ${test.count()}")

      val allTrainingData = train map { window =>
        val x:DenseVector[Double] = denseFeaturize(window.win.sequence)
        val y:DenseVector[Double] = labelVectorizer(window.label.toInt)
        val xtx = x * x.t
        val xty = x * y.t
        (x, y, xtx, xty)
        }

        val XTXStart = System.nanoTime
        val XTX = allTrainingData.map(_._3)
        val XTXloc = XTX.treeReduce({ (a:DenseMatrix[Double],b:DenseMatrix[Double]) =>
          a += b
        })
        println(s"XTX Computation took ${timeElapsed(XTXStart)}")
    }
  }

  def loadTsv(sc: SparkContext, filePath: String): RDD[(ReferenceRegion, Int)] = {
    val rdd = sc.textFile(filePath).filter(!_.contains("start"))
    rdd.map(line=> {
      val parts = line.split("\t")
      /* TODO: ignoring cell types for now */
      val label = parts.slice(3,parts.size).map(extractLabel(_)).reduceLeft(_ max _)
      (ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong), label)
    })

  }

  def timeElapsed(ns: Long) : Double = (System.nanoTime - ns).toDouble / 1e9

  def extractLabel(s: String): Int= {
    s match {
      case "A" => -1 // ambiguous
      case "U" => 0  // unbound
      case "B" => 1  // bound
      case _ => throw new IllegalArgumentException(s"Illegal label ${s}")
    }
  }
}
