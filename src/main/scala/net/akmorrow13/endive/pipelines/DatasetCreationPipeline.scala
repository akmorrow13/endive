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

import java.io.File
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.{Chromosomes, CellTypes, TranscriptionFactors}
import net.akmorrow13.endive.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD


object DatasetCreationPipeline extends Serializable  {

  /**
   * A very basic dataset creation pipeline that *doesn't* featurize the data
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
      val conf = new SparkConf().setAppName("ENDIVE")
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  /**
   *
   * @param sc
   * @param conf
   */
  def run(sc: SparkContext, conf: EndiveConf) {

    println("STARTING DATA SET CREATION PIPELINE")
    // create new sequence with reference path
    val referencePath = conf.reference
    if (referencePath == null)
      throw new Exception("referencepath not defined")
    val labelsPath = conf.labels
    if (labelsPath == null)
      throw new Exception("chipseq labels not defined")
    
    val fs: FileSystem = FileSystem.get(new Configuration())
    val labelStatus = fs.listStatus(new Path(labelsPath))
    println(s"first label file: ${labelStatus.head.getPath.getName}")

    val length = 200
    val half = length/2
      for (i <- labelStatus) {
        val file: String = i.getPath.toString
        try {

          val bed = sc.loadFeatures(file)
          bed.sequences.records.foreach(r => println(r.name, r.length))

          val labelBed = bed.rdd.cache()
          println(s"positive peak count in ${file}: ${labelBed.count}")

          // filter out peaks too short. take middle parts of peaks
          val filteredByLength = labelBed.filter(r => (r.end - r.start) >= length).map(r => {
            val mid = r.start + r.end / 2 + r.start
            r.setStart(mid - half)
            r.setEnd(mid + half)
            ReferenceRegion(r.getContigName, r.start, r.end)
          })

          // get negative peaks
          val collectedPeaks: Array[ReferenceRegion] = filteredByLength.collect()

          println(s"positive peak count after filtering by length: ${collectedPeaks.length}")

          val collectedPeaksB = sc.broadcast(collectedPeaks)
          val chrs = collectedPeaks.map(r => r.referenceName).distinct

          println("chrs with data:")
          chrs.foreach(println)

          val lengths = getSequenceDictionary(referencePath).records.filter(r => chrs.contains(r))

          // get negatives by filtering out regions that overlap positive peaks
          val negatives =
            sc.parallelize(lengths).repartition(lengths.length).flatMap(r => {
              List.range(1, r.length.toInt, length).map(i => ReferenceRegion(r.name, i.toLong, i + length))
                .filter(n => collectedPeaksB.value.filter(p => p.overlaps(n)).isEmpty)
            }).map(r => (r, 0)).filter(r => r._1.end < bed.sequences.apply(r._1.referenceName).get.length)

          // all points as referenceRegions
          val allPoints = negatives.union(filteredByLength.map(r => (r, 1)))


          val positiveTrain = allPoints.filter(_._2 == 1).mapPartitions({ part =>
            val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
            part.map { r =>
              val sequence = reference.extract(r._1)
              NucleotideContigFragment.newBuilder().setContig(Contig.newBuilder().setContigName(r._1.referenceName).build)
                .setFragmentSequence(sequence)
                .setFragmentStartPosition(r._1.start)
                .setFragmentEndPosition(r._1.end)
                .build()
            }
          }).map(r => (s"r.getFragmentSequence,1"))

          val negativeTrain = allPoints.filter(_._2 == 0).mapPartitions({ part =>
            val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
            part.map { r =>
              val sequence = reference.extract(r._1)
              NucleotideContigFragment.newBuilder().setContig(Contig.newBuilder().setContigName(r._1.referenceName).build)
                .setFragmentSequence(sequence)
                .setFragmentStartPosition(r._1.start)
                .setFragmentEndPosition(r._1.end)
                .build()
            }
          }).map(r => (s"r.getFragmentSequence,1"))

          println(negativeTrain.first())
          positiveTrain.union(negativeTrain).repartition(1).saveAsTextFile(conf.getFeaturizedOutput)

        }
      }
  }



  def getSequenceDictionary(referencePath: String): SequenceDictionary = {
    val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
    val seqRecords = reference.sequences.records
    new SequenceDictionary(seqRecords)
  }


  def extractSequencesAndLabels(referencePath: String, regionsAndLabels: RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)], placeHolder: Boolean = false): RDD[LabeledWindow]  = {
    /* TODO: This is a kludge that relies that the master + slaves share NFS
     * but the correct thing to do is to use scp/nfs to distribute the sequence data
     * across the cluster
     */
    // if sequences isnt required
    if (placeHolder) {
      regionsAndLabels.mapPartitions { part =>
        part.map { r =>
          val startIdx = r._3.start
          val endIdx = r._3.end
          val sequence = "N"
          val label = r._4
          val win: Window = Window(r._1, r._2, r._3, sequence)
          LabeledWindow(win, label)
        }
      }
    } else {
      regionsAndLabels.mapPartitions { part =>
        val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
        part.map { r =>
          val startIdx = r._3.start
          val endIdx = r._3.end
          val sequence = reference.extract(r._3)
          val label = r._4
          val win: Window = Window(r._1, r._2, r._3, sequence)
          LabeledWindow(win, label)
        }
      }
    }


  }

}
