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

import java.io.File
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.{Chromosomes, CellTypes, TranscriptionFactors}
import net.akmorrow13.endive.processing.Sequence
import net.akmorrow13.endive.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{ReferenceContigMap, ReferenceFile, TwoBitFile}
import org.bdgenomics.formats.avro._
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object SingleTFDatasetCreationPipeline extends Serializable  {

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

  def run(sc: SparkContext, conf: EndiveConf) {

    println("STARTING DATA SET CREATION PIPELINE")
    // create new sequence with reference path
    val referencePath = conf.reference

    // load dnase path for all narrowpeak files
    val dnasePath = conf.dnaseNarrow

    // load dnase paths for all dnase mabs
    val dnaseBamsPath = conf.dnaseBams

    // load chip seq labels from 1 file
    val labelsPath = conf.labels

    // create sequence dictionary
    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    val fs: FileSystem = FileSystem.get(new Configuration())
    val dnaseNarrowStatus = fs.listStatus(new Path(dnasePath))

    val (train: RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)], cellTypes: Array[CellTypes.Value]) = Preprocess.loadLabels(sc, labelsPath, 40)
    train
	.setName("Raw Train Data").cache()

    println(train.count, train.partitions.length)
    val tf = train.first._1
    println(s"celltypes for tf ${tf}:")
    cellTypes.foreach(println)

    // Load DNase data of (cell type, peak record)
    val dnaseFiles = dnaseNarrowStatus.filter(r => {
      val cellType = Dataset.filterCellTypeName(r.getPath.getName.split('.')(1))
      cellTypes.map(_.toString).contains(cellType)
    })

    // load peak data from dnase
    val dnase: RDD[(CellTypes.Value, PeakRecord)] = Preprocess.loadPeakFiles(sc, dnaseFiles.map(_.getPath.toString))
     .filter(r => Chromosomes.toVector.contains(r._2.region.referenceName)) 
     .cache()

    println("Reading dnase peaks")
    println(dnase.count)

    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] = extractSequencesAndLabels(referencePath, train).cache()
    println("labeled window count", sequences.count)

    val cellTypeInfo = new CellTypeSpecific(Dataset.windowSize, Dataset.stride, dnase, sc.emptyRDD[(CellTypes.Value, RNARecord)], sd)

    val fullMatrix: RDD[LabeledWindow] = cellTypeInfo.joinWithDNase(sequences)

    println("Now matching labels with reference genome")
    println(fullMatrix.count)

    println("Now saving to disk")
    fullMatrix.map(_.toString).saveAsTextFile(conf.aggregatedSequenceOutput + tf)
  }


  def extractSequencesAndLabels(referencePath: String, regionsAndLabels: RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)]): RDD[LabeledWindow]  = {
    /* TODO: This is a kludge that relies that the master + slaves share NFS
     * but the correct thing to do is to use scp/nfs to distribute the sequence data
     * across the cluster
     */
   	
    regionsAndLabels.mapPartitions( part => {
        val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
        part.map { r =>
          val startIdx = r._3.start
          val endIdx = r._3.end
          val sequence = reference.extract(r._3)
	  val label = r._4
          val win = Window(r._1, r._2, r._3, sequence)
          LabeledWindow(win, label)
       }
      })
  }

}
