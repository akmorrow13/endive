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


object TestSingleTFDatasetCreationPipeline extends Serializable  {

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
    val dnaseNarrowPath = conf.dnaseNarrow

    // load dnase paths for all dnase mabs
    val dnaseBamsPath = conf.dnaseBams

    val testCellType = CellTypes.liver

    // create sequence dictionary
    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    val stringChrs = Dataset.heldOutChrs.map(_.toString)

    val heldOutChrs = sd.records.filter(r => stringChrs.contains(r.name))
    assert(heldOutChrs.length == 3)

    // generate all sliding windows for whole genome
    val sequences = sc.parallelize(heldOutChrs).repartition(heldOutChrs.length)
    sequences.foreachPartition(r => println(r.length))
    assert(sequences.partitions.length == 3)

    // extract sequences
    val windows: RDD[LabeledWindow] =
      sequences
          .mapPartitions(iter => {
            if (iter.hasNext) {
              val f = iter.next() // should only be one element in each partition
              Array.range(0, f.length.toInt-Dataset.windowSize, Dataset.stride)
                .map(r => ReferenceRegion(f.name, r.toLong, r.toLong + Dataset.windowSize)).toIterator
            } else Iterator.empty
          }).repartition(30)
          .mapPartitions(iter => {
            val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
            iter.map { r =>
              LabeledWindow(Window(TranscriptionFactors.Any, testCellType, r, reference.extract(r), 0), -10) // no labels for test, so -10
            }
          }).repartition(500).setName("windows").cache()

    println("labeled window count", windows.count)

    // save sequences
    println("Now saving sequences to disk")
    windows.map(_.toString).saveAsTextFile(conf.aggregatedSequenceOutput + "test/sequence_windows")

    // now join with narrow peak dnase
    val fs: FileSystem = FileSystem.get(new Configuration())
    val dnaseNarrowStatus = fs.listStatus(new Path(dnaseNarrowPath))


    var fullMatrix: RDD[LabeledWindow] = {
      // Load DNase data of (cell type, peak record)
      val dnaseFiles = dnaseNarrowStatus.filter(r => {
        val cellType = Dataset.filterCellTypeName(r.getPath.getName.split('.')(1))
        cellType == testCellType
      })

      // load peak data from dnase
      val dnase: RDD[(CellTypes.Value, PeakRecord)] = Preprocess.loadPeakFiles(sc, dnaseFiles.map(_.getPath.toString))
        .filter(r => Chromosomes.toVector.contains(r._2.region.referenceName))
        .cache()

      println("Reading dnase peaks")
      println(dnase.count)

      val cellTypeInfo = new CellTypeSpecific(Dataset.windowSize, Dataset.stride, dnase, sc.emptyRDD[(CellTypes.Value, RNARecord)], sd)
      cellTypeInfo.joinWithDNase(windows)
    }

    fullMatrix.setName("fullmatrix_with_dnasePeaks").cache()
    fullMatrix.count()
    windows.unpersist()

    // join with dnase bams, if present
    fullMatrix = {
      // load cuts from AlignmentREcordRDD. filter out only cells of interest
      val coverage = Preprocess.loadDnase(sc, dnaseBamsPath, Array(testCellType))
      coverage.rdd.cache()
      coverage.rdd.count
      VectorizedDnase.joinWithDnaseBams(sc, sd, fullMatrix, coverage)
    }

    println("Now saving to disk")
    fullMatrix.map(_.toString).saveAsTextFile(conf.aggregatedSequenceOutput + "test/" + testCellType.toString)
  }

}
