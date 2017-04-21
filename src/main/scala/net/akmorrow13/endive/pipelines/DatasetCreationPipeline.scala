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
    
    // challenge parameters
    val windowSize = 200
    val stride = 50

//    val fs: FileSystem = FileSystem.get(new Configuration())
//    val labelStatus = fs.listStatus(new Path(labelsPath))
//    println(s"first label file: ${labelStatus.head.getPath.getName}")

    val length = 200
    val half = length/2
//      for (i <- labelStatus) {
//        val file: String = i.getPath.toString
	val file = labelsPath
        try {

          println(s"file: ${file}")

	  val sequences = getSequenceDictionary(referencePath)

          val bed = sc.loadFeatures(file)
      
          val labelBed = bed.rdd.cache()
          println(s"positive peak count in ${file}: ${labelBed.count}")

          // filter out peaks too short. take middle parts of peaks
          val filteredByLength = labelBed.filter(r => (r.end-r.start) >= length).map(r => {
            val mid = (r.end-r.start) / 2 + r.start
	    val s = (mid - half)
            val e = (mid+half)
            ReferenceRegion(r.getContigName, s, e)
          })
         
          // get negative peaks
          val collectedPeaks: Array[ReferenceRegion] = filteredByLength.collect()
	  println(collectedPeaks.head)

          val chrs = collectedPeaks.map(r => r.referenceName).distinct
	  println(s"positive peak count after filtering by length: ${collectedPeaks.length}")

          val collectedPeaksB = sc.broadcast(collectedPeaks)
          val lengths = getSequenceDictionary(referencePath).records.filter(r => chrs.contains(r.name))
          lengths.foreach(println)

          // get negatives by filtering out regions that overlap positive peaks
          val negatives =
            sc.parallelize(lengths).repartition(lengths.length).flatMap(r => {
              List.range(1, r.length.toInt, length).map(i => ReferenceRegion(r.name, i.toLong, i+length))
                  .filter(n => collectedPeaksB.value.filter(p => p.overlaps(n)).isEmpty)
            }).map(r => (r, 0)) 

          // all points as referenceRegions
          val allPoints = negatives.union(filteredByLength.map(r => (r, 1)))
    		.filter(r => r._1.end <= sequences(r._1.referenceName).get.length)
          println(s"all points after filter: ${allPoints.count}")
          val testChrs = List("chr18", "chr19")

          // save bed file for train and test
          val train = allPoints.filter(r => !testChrs.contains(r._1.referenceName)).repartition(40)
          val test = allPoints.filter(r => testChrs.contains(r._1.referenceName)).repartition(40)

/*
          train.map(r => s"${r._1.referenceName},${r._1.start},${r._1.end},${r._2}").saveAsTextFile(conf.getFeaturesOutput + "_train")
          test.map(r => s"${r._1.referenceName},${r._1.start},${r._1.end},${r._2}").saveAsTextFile(conf.getFeaturesOutput + "_test")
*/

          // dataset 1: get sequences for test positives
          val testFastaPositive = test.filter(_._2 == 1).mapPartitions { part =>
            val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
            part.map { r =>
                val sequence = reference.extract(r._1)
                NucleotideContigFragment.newBuilder().setContig(Contig.newBuilder().setContigName(r._1.referenceName).build)
                  .setFragmentSequence(sequence)
                  .setFragmentStartPosition(r._1.start)
                  .setFragmentEndPosition(r._1.end)
                  .build()
            }
          }

          // save test positives
  //        new NucleotideContigFragmentRDD(testFastaPositive, bed.sequences).transform(rdd => rdd.repartition(1)).save(conf.getFeaturesOutput + "_positive_test.fasta")

          // dataset 1: get sequences for test positives
          val testFastaNegative = test.filter(_._2 == 0).mapPartitions { part =>
            val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
            part.map { r =>
              val sequence = reference.extract(r._1)
              NucleotideContigFragment.newBuilder().setContig(Contig.newBuilder().setContigName(r._1.referenceName).build)
                .setFragmentSequence(sequence)
                .setFragmentStartPosition(r._1.start)
                .setFragmentEndPosition(r._1.end)
                .build()
            }
          }

          // save test positives
          // new NucleotideContigFragmentRDD(testFastaNegative, bed.sequences).transform(rdd => rdd.repartition(1)).save(conf.getFeaturesOutput + "_negative_test.fasta")


          // save files with train dist matching test up to 100000 points
          val pointCount = train.count()
          println(s" filtering total count from ${pointCount} to 100,000 ...")


          val positiveTrain = train.filter(_._2 == 1).mapPartitions { part =>
            val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
            part.map { r =>
              val sequence = reference.extract(r._1)
              NucleotideContigFragment.newBuilder().setContig(Contig.newBuilder().setContigName(r._1.referenceName).build)
                .setFragmentSequence(sequence)
                .setFragmentStartPosition(r._1.start)
                .setFragmentEndPosition(r._1.end)
                .build()
            }
          }

          val negativeTrain = train.filter(_._2 == 0).mapPartitions { part =>
            val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
            part.map { r =>
              val sequence = reference.extract(r._1)
              NucleotideContigFragment.newBuilder().setContig(Contig.newBuilder().setContigName(r._1.referenceName).build)
                .setFragmentSequence(sequence)
                .setFragmentStartPosition(r._1.start)
                .setFragmentEndPosition(r._1.end)
                .build()
            }
          }.filter(r => !r.getFragmentSequence.contains("N"))


          val positiveCount = positiveTrain.count()
          val negativeCount = negativeTrain.count()

          println(s"positive train count: ${positiveCount}, negative train count: ${negativeCount}")

/*
          // calculate train skew and use this to save training set
          val skew: Double = positiveCount.toDouble / negativeCount.toDouble
          println(s"skew of positives to negatives in train set: ${skew}")

          // save positives for 10,000 points
          var positiveSample = (skew * 10000).toInt
          var negativeSample = 10000-positiveSample
          println(s"positives for 10000: ${positiveSample}, negatives: ${negativeSample}")

          new NucleotideContigFragmentRDD(sc.parallelize(positiveTrain.takeSample(false, positiveSample.toInt), 1), bed.sequences)
            .save(conf.getFeaturesOutput + s"_positive_skew_${skew}_10000_train.fasta")

          new NucleotideContigFragmentRDD(sc.parallelize(negativeTrain.takeSample(false, negativeSample.toInt), 1), bed.sequences)
            .save(conf.getFeaturesOutput + s"_negative_skew_${skew}_10000_train.fasta")


          // save positives for 50,000 points
          positiveSample = (skew * 50000).toInt
          negativeSample = 50000-positiveSample

          println(s"positives for 50000: ${positiveSample}, ${negativeSample}")

          new NucleotideContigFragmentRDD(sc.parallelize(positiveTrain.takeSample(false, positiveSample.toInt), 1), bed.sequences)
            .save(conf.getFeaturesOutput + s"_positive_skew_${skew}_50000_train.fasta")

          new NucleotideContigFragmentRDD(sc.parallelize(negativeTrain.takeSample(false, negativeSample.toInt), 1), bed.sequences)
            .save(conf.getFeaturesOutput + s"_negative_skew_${skew}_50000_train.fasta")


          // save positives for 100,000 points
          positiveSample = (skew * 100000).toInt
          negativeSample = 100000-positiveSample

          println(s"positives for 100000: ${positiveSample}, ${negativeSample}")

          new NucleotideContigFragmentRDD(sc.parallelize(positiveTrain.takeSample(false, positiveSample.toInt), 1), bed.sequences)
            .save(conf.getFeaturesOutput + s"_positive_skew_${skew}_100000_train.fasta")

          new NucleotideContigFragmentRDD(sc.parallelize(negativeTrain.takeSample(false, negativeSample.toInt), 1), bed.sequences)
            .save(conf.getFeaturesOutput + s"_negative_skew_${skew}_100000_train.fasta")




          // data set 2: save files with train dist maxing out the number of positives

          // save full positives train
          new NucleotideContigFragmentRDD(positiveTrain, bed.sequences).transform(rdd => rdd.repartition(1)).save(conf.getFeaturesOutput + "_positive_full_100000_train.fasta")
*/
          val negativeSample = 200000-positiveCount.toInt

          new NucleotideContigFragmentRDD(sc.parallelize(negativeTrain.takeSample(false, negativeSample.toInt), 1), bed.sequences)
            .save(conf.getFeaturesOutput + "_negative_full_200000_train.fasta")

        }

 //       }
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
