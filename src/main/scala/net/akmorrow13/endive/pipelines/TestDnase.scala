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
          val windows = LabeledWindowLoader(conf.featuresOutput, sc).filter(_.label >= 0).cache()
          var positives = windows.filter(r => r.label == 1)
          var negatives = windows.filter(r => r.label == 0)
          // print original statisics
          println(s"ORIGINAL STATS: total windows: ${windows.count}, positive count: ${positives.count}, negative count: ${negatives.count} ")

          // consolidate positives
          positives =
            positives.groupBy(_.win.region.referenceName)
              .flatMap(r => {
                val cellTypes = r._2.toList.groupBy(_.win.cellType)

                cellTypes.flatMap(list => {
                  val sorted: List[LabeledWindow] = list._2.sortBy(_.win.region)
                  var newWindows: Array[LabeledWindow] = Array.empty[LabeledWindow]
                  sorted.foreach(r => {
                    if (!newWindows.isEmpty && r.win.region.overlaps(newWindows.last.win.region)) {
                      // merge into last element
                      val elem = newWindows.last
                      val region = r.win.region.merge(elem.win.region)
                      val dnaseCount = r.win.dnasePeakCount + elem.win.dnasePeakCount
                      newWindows(newWindows.length-1) = LabeledWindow(elem.win.setRegion(region).setDnaseCount(dnaseCount), 1)

                    } else newWindows = newWindows :+ r
                  })
                  newWindows
                })
            })

          negatives = negatives.filter(_.win.dnasePeakCount > 0)

          val positivesWithoutPeaks = positives.filter(_.win.dnasePeakCount == 0)
          val positivesWithPeaks = positives.filter(_.win.dnasePeakCount > 0)

          val posLen = positives.map(_.win.region.length()).mean()
          println(s"average region size of positives: ${posLen}")

          println(s"counting number of (negatives + positives): ${negatives.count} + ${positives.count} for total dataset")
          println(s"number of positives that do not have peaks: ${positivesWithoutPeaks.count}, number of positives that do have peaks: ${positivesWithPeaks.count}")

        }

}





