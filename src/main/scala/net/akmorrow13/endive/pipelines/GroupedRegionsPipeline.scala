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

import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.utils._
import com.github.fommil.netlib.BLAS
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging

object GroupedRegionsPipeline extends Serializable with Logging {

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

    val windowLoc = conf.featuresOutput
    val windows = LabeledWindowLoader(windowLoc, sc).filter(_.label >= 0).cache()
    val tf = windows.first.win.getTf.toString

    var positives = windows.filter(r => r.label == 1)
    var negatives = windows.filter(r => r.label == 0)
    // print original statisics
    println(s"ORIGINAL STATS for ${tf}: total windows: ${windows.count}, positive count: ${positives.count}, negative count: ${negatives.count} ")

    // merge positives together. filter out windows without peaks
    positives = mergeAdjacent(positives.map(_.win)).map(r => LabeledWindow(r, 1))
    val posLostFromPeaks = positives.filter(_.win.getDnasePeakCount == 0).count

    positives = positives.filter(_.win.getDnasePeakCount > 0)

    // merge adjacent negatives together. filter out windows without peaks
    negatives = mergeAdjacent(negatives.filter(_.win.getDnasePeakCount > 0).map(_.win)).map(r => LabeledWindow(r, 0))

    // truncate positives and negatives to normalize the length
    val windowLength = 300
    val half = windowLength/2

    val newSet = positives.union(negatives).filter(_.win.getRegion.length >= windowLength).map(r => {
      // find middle
      val center = r.win.getRegion.length()/2 + r.win.getRegion.start
      LabeledWindow(r.win.slice(center - half, center + half), r.label)
    })

    val positivesWithoutPeaks = positives.filter(_.win.getDnasePeakCount == 0)
    val positivesWithPeaks = positives.filter(_.win.getDnasePeakCount > 0)

    println(s"counting number of (negatives + positives): ${newSet.filter(_.label == 0).count} + ${newSet.filter(_.label == 1).count} for total dataset")
    println(s"number of positives that we lost from thresholding windowsize to >= ${windowLength}: ${positives.filter(_.win.getRegion.length < windowLength).count}")
    println(s"number of positives that we lost from eliminating sites without peaks: ${posLostFromPeaks}")

    // save to disk
    newSet.map(_.toString).repartition(1).saveAsTextFile(conf.featurizedOutput + tf)
  }


  /**
   * Merges adjacent windows by cell type
   *
   * @param data
   * @return
   */
  def mergeAdjacent(data: RDD[Window]): RDD[Window] = {
    data.groupBy(_.getRegion.referenceName)
      .flatMap(r => {
        val cellTypes = r._2.toList.groupBy(_.getCellType)

        cellTypes.flatMap(list => {
          val sorted: List[Window] = list._2.sortBy(_.getRegion)
          var newWindows: Array[Window] = Array.empty[Window]
          sorted.foreach(r => {
            if (!newWindows.isEmpty && r.getRegion.overlaps(newWindows.last.getRegion)) {
              // merge into last element
              newWindows(newWindows.length-1) = newWindows.last.merge(r)
            } else newWindows = newWindows :+ r
          })
          newWindows
        })
      })
  }

  def printStats(sc: SparkContext, conf: EndiveConf): Unit = {
    val windows = LabeledWindowLoader(conf.featuresOutput, sc).filter(_.label >= 0).cache()
    var positives = windows.filter(r => r.label == 1)
    var negatives = windows.filter(r => r.label == 0)
    // print original statisics
  
    println(s"ORIGINAL STATS: total windows: ${windows.count}, positive count: ${positives.count}, negative count: ${negatives.count} ")

    // consolidate positives
    positives = mergeAdjacent(positives.map(_.win)).map(r => LabeledWindow(r, 1))

    // consolidate negatives
    negatives = mergeAdjacent(negatives.filter(_.win.getDnasePeakCount > 0).map(_.win)).map(r => LabeledWindow(r, 0))

    val positivesWithoutPeaks = positives.filter(_.win.getDnasePeakCount == 0)
    val positivesWithPeaks = positives.filter(_.win.getDnasePeakCount > 0)

    val posLen = positives.map(_.win.getRegion.length())
    println(s"average region size of positives: ${posLen}")

    val negLen = negatives.map(_.win.getRegion.length()).mean()
    println(s"average region size of positives: ${negLen}")

    println(s"counting number of (negatives + positives): ${negatives.count} + ${positives.count} for total dataset")
    println(s"number of positives that do not have peaks: ${positivesWithoutPeaks.count}, number of positives that do have peaks: ${positivesWithPeaks.count}")

  }

}
