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

import breeze.linalg.DenseVector
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.{CellTypes, TranscriptionFactors}
import net.akmorrow13.endive.utils.{Window, LabeledWindow, LabeledWindowLoader}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.BinaryColumn
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.io.api.Binary
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion, Coverage}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.{Strand, Feature}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import org.bdgenomics.utils.misc.Logging

object MergeLabelsAndSequences extends Serializable with Logging {

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
      val rootLogger = Logger.getRootLogger()
      rootLogger.setLevel(Level.INFO)
      val conf = new SparkConf().setAppName("ENDIVE:SingleTFDatasetCreationPipeline")
      conf.setIfMissing("spark.master", "local[4]")
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  /**
   * Merges alignment cuts by counting all cuts at one site
   * @param sc
   * @param conf
   */
  def run(sc: SparkContext, conf: EndiveConf) {
    mergeDnase(sc, conf)
    sys.exit(0)

    val labelBed = conf.getLabels       // regions (only has POSITIVE strands)
    val sequenceFile = conf.getSequenceLoc // sequence and labels
    val featuresOutput = conf.getFeaturesOutput

    // expand out labels for forward and reverse strand
    val l = sc.loadFeatures(labelBed)

    val labels = l.rdd.flatMap(r => {
          val start = r.getStart - 400 // expand region size out to 1000
          val end = r.getEnd + 400
          val forward = Feature.newBuilder(r)
            .setStrand(Strand.FORWARD)
            .setStart(start)
            .setEnd(end)
            .build()
          val reverse = Feature.newBuilder(forward)
            .setStrand(Strand.REVERSE)
            .build()
          Iterable(forward, reverse)
        }).sortBy(f => ReferenceRegion.stranded(f)).zipWithIndex().map(r => (r._2, r._1))
    labels.take(2).foreach(println)

    val sequences = sc.textFile(sequenceFile).zipWithIndex().map(r => (r._2, r._1))

    val labelsCount = labels.count()
    val sequenceCount = sequences.count()
    require(labelsCount == sequenceCount, s"required that labelsCount (${labelsCount}) ==" +
      s" sequenceCount (${sequenceCount})")

    // join sequences and labels and save
    println(s"saving to ${conf.getNumPartitions} partitions")
    val zipped = labels.join(sequences, conf.getNumPartitions).sortBy(_._1)


    val strings = zipped.map(r =>
      s"${r._2._1.getContigName},${r._2._1.getStart},${r._2._1.getEnd},${r._2._1.getStrand},${r._2._2}")

    strings.saveAsTextFile(featuresOutput)

    // save as bed file for multiple cell types for train
    if (!conf.testBoard) {
      // get array of positions with relevant cell types
      // GM12878,h1-hESC,K562,HCT-116
      val cellTypes = Array(("GM12878",53),("H1-hESC",54),("K562",61),("HCT-116",80))

      for (cellType <- cellTypes) {
        val rdd = zipped.map(r => {
          Feature.newBuilder(r._2._1)
              .setScore(r._2._2.split(',')(cellType._2+1).toInt.toDouble)
              .build()
        }).filter(_.getScore() > 0)
	      println(s"saving celltype ${cellType._1} for ${rdd.count()} sequences")
        new FeatureRDD(rdd, l.sequences)
          .saveAsBed(s"featuresOutput_${cellType._1}.adam")
      }
    }
  }

  // merge + and - cuts separately
  def mergeDnase(sc: SparkContext, conf: EndiveConf): Unit = {


    val coveragePath = conf.getCutmapInputPath
    val featuresPath = conf.getWindowLoc
    val output = conf.getFeaturesOutput

    val fs: FileSystem = FileSystem.get(new Configuration())
    val alignmentStatus = fs.listStatus(new Path(coveragePath))
    println(s"num partitions: ${conf.numPartitions}")
    println(s"first alignment file: ${alignmentStatus.head.getPath.getName}, first cell type: ${alignmentStatus.head.getPath.getName.split('.')(2)}")

    val features = sc.textFile(featuresPath).map(r => {
      val arr = r.split(',')
      val region = new ReferenceRegion(arr(0), arr(1).toLong, arr(2).toLong, Strand.valueOf(arr(3)))
      val sequence = arr(4)
      val win = Window(TranscriptionFactors.Any, CellTypes.Any, region, sequence)
      LabeledWindow(win, 0)
    })
    val first = features.first

    features.cache()
    val featureCount = features.count

    val replicates = Array("liver:DNASE.liver.biorep1.techrep2.bam",
      "inducedpluripotentstemcell:DNASE.induced_pluripotent_stem_cell.biorep2.techrep2.bam",
      "Panc1:DNASE.Panc1.biorep2.techrep1.bam",
      "PC3:DNASE.PC-3.biorep1.techrep1.bam",
      "MCF7DNASE.MCF-7.biorep1.techrep4.bam",
      "K562:DNASE.K562.biorep2.techrep6.bam",
      "IMR90:DNASE.IMR90.biorep1.techrep1.bam",
      "HepG2:DNASE.HepG2.biorep2.techrep3.bam",
      "HeLaS3:DNASE.HeLa-S3.biorep1.techrep2.bam",
      "HCT116:DNASE.HCT116.biorep1.techrep1.bam",
      "H1hESC:DNASE.H1-hESC.biorep1.techrep2.bam",
      "GM12878:DNASE.GM12878.biorep1.techrep1.bam", "A549:DNASE.A549.biorep1.techrep3.bam")

    val partitionCount = (featureCount / conf.numPartitions).toInt

    val alignmentSequences = sc.loadParquetAlignments(alignmentStatus.head.getPath.toString).sequences
    // get sequence records in file
    val chrs = features.map(_.win.getRegion.referenceName).distinct().collect

    val sequences = new SequenceDictionary(alignmentSequences.records.filter(r => chrs.contains(r.name)))

    for (i <- alignmentStatus) {
      val file: String = i.getPath.toString
      val cellType = i.getPath.getName.split('.')(2)
      var coverage = sc.loadParquetAlignments(file).transform(_.filter(r => chrs.contains(r.getContigName) && r.start >= 0).map(r => {
        r.setReadMapped(true)
        r
      }).repartition(conf.numPartitions * 6))
      coverage = coverage.transform(rdd => rdd.filter(r => replicates.contains(r.getAttributes)))

      println(s"coverage count: ${coverage.rdd.count}")

      println(s"overlap: ${coverage.rdd.filter(r => ReferenceRegion.stranded(r).overlaps(first.win.getRegion)).count}")
      val joined = VectorizedDnase.joinWithDnaseBams(sc, sequences, features, coverage, partitionCount)
        .map(r =>
          s"${r.win.getRegion.referenceName},${r.win.getRegion.start},${r.win.getRegion.end},${r.win.getRegion.strand},${r.win.getDnase.toArray.toList.mkString(",")}")
      println(s"saving to ${output}, first: ${joined.first}")
      joined.repartition(conf.numPartitions).saveAsTextFile(s"${output}${cellType}")
    }
  }


  /** Dnase is of form chr7,31439600,31440600,FORWARD,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0, ...
    * where vector is length of region
    * @param sequencePath sequence data to be joined
    * @param dnasePath path to dnase
    * @return
    */
  def joinDnaseAndSequence(sc: SparkContext, sequencePath: String,
                           dnasePath: String, outputPath: String, partitions: Int = 100): RDD[LabeledWindow] = {

    val cellType = CellTypes.getEnumeration(dnasePath.split("/").last)

    val dnase = sc.textFile(dnasePath).map(r => {
      val arr = r.split(',')
      val region = ReferenceRegion(arr(0),arr(1).toLong, arr(2).toLong, Strand.valueOf(arr(3)))
      val vector = DenseVector(arr.slice(4, arr.length).map(_.toDouble))
      (region, vector)
    })

    val sequences: RDD[(ReferenceRegion, (String, Array[Int]))] = sc.textFile(sequencePath)
      .map(r => {
        val arr = r.split(',')
        val region = ReferenceRegion(arr(0),arr(1).toLong, arr(2).toLong, Strand.valueOf(arr(3)))
        val sequence = arr(4)
        val labels = arr.slice(5,arr.length).map(r => r.toInt)
        (region, (sequence, labels))
      })

    val joined = sequences.leftOuterJoin(dnase)
      .map(r => {
        val win = Window(TranscriptionFactors.Any, cellType, r._1, r._2._1._1, 0, r._2._2, None, None)
        LabeledWindow(win, r._2._1._2)
       })

    joined.repartition(partitions).map(r => r.toString).saveAsTextFile(outputPath)

    joined
  }
}
