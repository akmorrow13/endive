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
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.{ BlockLeastSquaresEstimator}
import nodes.util.{Cacher, MaxClassifier, ClassLabelIndicatorsFromIntLabels}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.formats.avro._
import org.bdgenomics.adam.rdd._
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import nodes.learning._
import breeze.stats._
import breeze.math._
import breeze.numerics._

import java.io.{File, BufferedWriter, FileWriter}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import scala.collection.mutable.ArrayBuffer
import nodes.stats._



object TestDnasePipeline extends Serializable with Logging {

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
          val windows = LabeledWindowLoader(conf.featuresOutput, sc)

          // filter by motifs
          val motifs: RDD[Feature] = conf.getMotifDBPath.split(",").map(file => {
            sc.textFile(file).filter(r => !r.contains("start"))
              .map(r => {
                val split = r.split('\t')
                Feature.newBuilder()
                  .setContigName(split(1))
                  .setStart(split(2).toLong - 10)
                  .setEnd(split(3).toLong + 10)
                  .setScore(split(6).toDouble).build()
              })
          }).reduce(_ union _)

          val motifB = sc.broadcast(motifs.collect)

          val motifWindows: RDD[(Array[Feature], LabeledWindow)] = windows.map(r => {
            (motifB.value.filter(f => ReferenceRegion.unstranded(f).overlaps(r.win.getRegion)), r)
          }).cache()

          println(s"total windows: ${motifWindows.count}, positives ${motifWindows.filter(r => r._2.label == 1).count}, negative count: ${motifWindows.filter(r => r._2.label == 0).count} ")
          println(s"average negative dnase: ${motifWindows.filter(r => r._2.label == 0).map(r => r._2.win.getDnase.sum).mean()}")
          println(s"average positive dnase: ${motifWindows.filter(r => r._2.label == 1).map(r => r._2.win.getDnase.sum).mean()}")

          println(s"counting number of positives with no dnase, no motifs, that are positive: ${motifWindows.filter(r => r._1.length >= 0 && r._2.win.getDnase.sum == 0 && r._2.label == 1).count}")
          println(s"counting number of positives with 1 dnase, no motifs, that are positive: ${motifWindows.filter(r => r._1.length >= 0 && r._2.win.getDnase.sum <= 1.0 && r._2.label == 1).count}")
          println(s"counting number of positives with 2 dnase, no motifs, that are positive: ${motifWindows.filter(r => r._1.length >= 0 && r._2.win.getDnase.sum <= 2.0 && r._2.label == 1).count}")

          println(s"counting number of negatives with any motifs: ${motifWindows.filter(r => r._1.length > 0).count}")

        }

}

