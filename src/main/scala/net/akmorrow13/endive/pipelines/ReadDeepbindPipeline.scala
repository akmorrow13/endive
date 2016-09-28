/**
 * Copyright 2016 Alyssa Morrow
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

import java.io.File

import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.{TranscriptionFactors, Preprocess, PeakRecord, CellTypes}
import net.akmorrow13.endive.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.GenomicRegionPartitioner
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import org.bdgenomics.utils.misc.Logging

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer


object ReadDeepbindPipeline extends Serializable with Logging {

  /**
   * A very basic dataset creation pipeline that *doesn't* featurize the data
   * but creates a csv of (Window, Label)
   *
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
      val conf = new SparkConf().setAppName("ENDIVE:SingleTFDatasetCreationPipeline")
      conf.setIfMissing("spark.master", "local[4]")
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {

    val labelsPath = conf.labels // this is the aggregated data
    val chipseq = conf.chipPeaks // this is the directory of the chipPeaks
    val referencePath = conf.reference
    if (labelsPath == null || chipseq == null || referencePath == null)
      throw new Exception("labels are null")

    println(labelsPath.split('/').last)
    val tf = TranscriptionFactors.withName(labelsPath.split('/').last)


    // create sequence dictionary
    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    val data: RDD[LabeledWindow] = sc.textFile(labelsPath)
      .map(s => LabeledWindowLoader.stringToLabeledWindow(s))
    .repartition(50)

    val positives = data.filter(_.label == 1)
    val negatives = data.filter(r => r.label == 0 &&  r.win.getDnase.size > 0)
      .sample(false, 0.3)

    // merge with conservative peaks and center at the peak
    val fs: FileSystem = FileSystem.get(new Configuration())
    val labelStatus = fs.listStatus(new Path(chipseq))
          .filter(_.getPath.toString.contains(tf.toString)) // get peaks for this tf



    // iterate for all cell Type
    for (i <- labelStatus) {
      val file: String = i.getPath.toString
      println(file)
      val cellType = CellTypes.getEnumeration(file.split("/").last.split('.')(1))
      println(s"creating data for celltype ${cellType}")

      // peaks for 1 cell type
      val peaks: Broadcast[Array[PeakRecord]] = sc.broadcast(Preprocess.loadPeaks(sc, file)
                  .map(_._2)
                  .collect)

      println(s"peak count ${peaks.value.length}")
      val half = 50

      // filter by current celltypes for positives and negatives
      val cellTypePositives = positives.filter(_.win.cellType == cellType)


      val cellTypeNegatives = negatives.filter(_.win.cellType == cellType)
        .map(r => (r.win.sequence.substring(100-half, 100+half), r.label)) //map to (sequence, label) pairs

      val peakRegions: RDD[ReferenceRegion] = cellTypePositives.map(r => peaks.value.find(p => p.region.overlaps(r.win.region)))
        .filter(r => r.isDefined)
        .map(r => r.get.region).distinct() // get all distinct regions
        .map(r => {
          val middle = (r.end - r.start)/2 + r.start
          ReferenceRegion(r.referenceName, middle - half, middle + half) // remap to center peak
        })

      val centeredPositives: RDD[(String, Int)] = DatasetCreationPipeline.extractSequences(referencePath, peakRegions)
        .map(r => (r._2, 1)) // map to sequences and positive labels

      println(s"celltpye positives and negatives ${centeredPositives.count}, ${cellTypeNegatives.count}")

      // save negatives and positives as tsv (tf, cellType, seq, label)
      val finalPositives = centeredPositives.collect

      // make sure you have the same number of positives and negatives
      val positiveCount = finalPositives.length
      val finalNegatives = cellTypeNegatives.takeSample(false, positiveCount)
      val fin = finalPositives.union(finalNegatives)

     val (posLen, negLen) = (cellTypeNegatives.first._1.length, finalPositives.head._1.length)
     println(s"lengths ${posLen}, ${negLen}")
     assert(posLen == negLen)

      // save csv file with labels
      var output = s"/home/eecs/akmorrow/ADAM/tfPaper/ENCODEFormatted/${tf.toString}.${cellType.toString}.csv"
      printToFile(new File(output)) { p =>
        fin.map(r => s"${r._1},${r._2}").foreach(p.println)
      }

      // save seq file without labels
      output = s"/home/eecs/akmorrow/ADAM/tfPaper/ENCODEFormatted/${tf.toString}.${cellType.toString}.seq"
      printToFile(new File(output)) { p =>
        fin.map(r => s"${r._1}").foreach(p.println)
      }
    }


  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  /**
   * Tail recursion for merging adjacent ReferenceRegions with the same value.
   *
   * @param iter partition iterator of ReferenceRegion and coverage values.
   * @param condensed Condensed iterator of iter with adjacent regions with the same value merged.
   * @return merged tuples of adjacent ReferenceRegions and coverage.
   */
  @tailrec def collapse(iter: Iterator[(ReferenceRegion, LabeledWindow)],
                                last: (ReferenceRegion, LabeledWindow),
                                condensed: List[(ReferenceRegion, LabeledWindow)]): Iterator[(ReferenceRegion, LabeledWindow)] = {
    if (!iter.hasNext) {
      // if lastCoverage has not yet been added, add to condensed
      val nextCondensed =
        if (condensed.map(r => r._1).filter(_.overlaps(last._1)).isEmpty) {
          last :: condensed
        } else {
          condensed
        }
      nextCondensed.toIterator
    } else {
      val cov = iter.next
      val rr = cov
      val lastRegion = last
      val (nextCoverage, nextCondensed) =
        if (rr._1.overlaps(lastRegion._1) && rr._2.win.cellType == lastRegion._2.win.cellType) {
          // merge sequences
          val hull = rr._1.hull(lastRegion._1)
          val seq =
            if (rr._1.compareTo(lastRegion._1) == -1) // rr is < last
              rr._2.win.sequence.substring(0, (lastRegion._1.start - rr._1.start).toInt) + lastRegion._2.win.sequence
            else
              lastRegion._2.win.sequence.substring(0, (rr._1.start - lastRegion._1.start).toInt)+ rr._2.win.sequence
          assert(seq.length == hull.length())
          val dnase = (rr._2.win.dnase ++ lastRegion._2.win.dnase).distinct
          val rnaseq = (rr._2.win.rnaseq ++ lastRegion._2.win.rnaseq).distinct
          val motifs = (rr._2.win.motifs ++ lastRegion._2.win.motifs).distinct
          val win = Window(rr._2.win.tf, rr._2.win.cellType, hull, seq, dnase,rnaseq,motifs)
          ((hull, LabeledWindow(win, rr._2.label)), condensed)
        } else {
          (cov, last :: condensed)
        }
      collapse(iter, nextCoverage, nextCondensed)
    }
  }

}
