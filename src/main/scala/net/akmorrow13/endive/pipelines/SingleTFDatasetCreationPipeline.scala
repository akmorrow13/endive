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
import net.akmorrow13.endive.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.bdgenomics.adam.rdd.feature.CoverageRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{TwoBitFile}
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
    val dnaseNarrowPath = conf.dnaseNarrow

    // load dnase paths for all dnase mabs
    val dnaseBamsPath = conf.dnaseBams

    // load chip seq labels from directory
    val labelsDir = conf.labels

    val fs: FileSystem = FileSystem.get(new Configuration())
    val labelsPaths = fs.listStatus(new Path(labelsDir)).map(r => r.getPath.toString).filter(_.endsWith(".tsv"))	

    // create sequence dictionary
    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    val dnaseNarrowStatus = fs.listStatus(new Path(dnaseNarrowPath))

    val savedTfs = fs.listStatus(new Path(conf.aggregatedSequenceOutput)).map(r => r.getPath.getName)
    println("saved TFS")
    savedTfs.foreach(println)

   for (labelsPath <- labelsPaths) {
      println(s"processing ${labelsPath}")
      val tfStr = labelsPath.split("/").last.split('.').head
      println(s"tf: ${tfStr}")

      val tf = TranscriptionFactors.withName(tfStr)
      println(tf)

      // if tf has not yet been saved
      if (!savedTfs.contains(tf.toString)) {

        // Define file outputs:
        // sequence output
        val sequenceOutput = conf.aggregatedSequenceOutput + "onlySequences/" + tf
        // final output
        val finalOutput = conf.aggregatedSequenceOutput + tf

        var (train: RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)], cellTypes: Array[CellTypes.Value]) = Preprocess.loadLabels(sc, labelsPath, 40)

        train.setName("Raw Train Data").cache()

        println(train.count, train.partitions.length)
        assert(tf.equals(train.first._1))
        println(s"celltypes for tf ${tf}:")
        cellTypes = cellTypes.filter(r => !r.equals(CellTypes.SKNSH))
        cellTypes.foreach(println)

        val sequences: RDD[LabeledWindow] =
          try {
            LabeledWindowLoader(sequenceOutput, sc)
          } catch {
            case e: Exception => {
              println(e.getMessage)
              // extract sequences from reference over all regions
              val sequences: RDD[LabeledWindow] = extractSequencesAndLabels(referencePath, train.repartition(50)).cache()
              println("labeled window count", sequences.count)

              // save sequences
              println("Now saving sequences to disk")
              sequences.map(_.toString).saveAsTextFile(sequenceOutput)
              sequences
            }
          }

        // merge in narrow files (Required)
        val fullMatrix: RDD[LabeledWindow] = {
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

            val cellTypeInfo = new CellTypeSpecific(Dataset.windowSize, Dataset.stride, dnase, sc.emptyRDD[(CellTypes.Value, RNARecord)], sd)
            cellTypeInfo.joinWithDNase(sequences)
          }

        fullMatrix.setName("fullmatrix_with_dnasePeaks").cache()
        fullMatrix.count()
        sequences.unpersist()

        // save sequences with narrow peak
        println("completed sequence and narrow peak integration")

        // join with dnase bams (Required)
        var fullMatrixWithBams: RDD[LabeledWindow] = null

    //    // TODO: START CODE REMOVE
    //    val fullMatrix = LabeledWindowLoader(labelsPath, sc)
    //    val cellTypes = fullMatrix.map(_.win.cellType).distinct.collect
    //    println("got cellTypes:")
    //    cellTypes.map(_.toString).foreach(println)
    //    val tfs = fullMatrix.map(_.win.getTf).distinct.collect
    //    assert(tfs.length == 1)
    //    val tf = tfs.head
    //    println(s"Transcription factor ${tf.toString}")
    //    // TODO: END REMOVE CODE

        // iterate through each cell type
        for (cellType <- cellTypes) {
          println(s"processing dnase for celltype ${cellType.toString}")

          // load cuts from CoverageRDD. filter out only cells of interest
          val (positiveCoverage: CoverageRDD, negativeCoverage: CoverageRDD) =
            Preprocess.loadDnase(sc, dnaseBamsPath, cellType)

          positiveCoverage.rdd.cache()
          positiveCoverage.rdd.count

          negativeCoverage.rdd.cache()
          negativeCoverage.rdd.count

          val newData = VectorizedDnase.joinWithDnaseCoverage(sc, sd,
            fullMatrix.filter(_.win.getCellType == cellType), positiveCoverage, negativeCoverage)

          if (fullMatrixWithBams == null)
            fullMatrixWithBams = newData
          else
            fullMatrixWithBams = fullMatrixWithBams.union(newData)
        }
        println("Now saving to disk")
        fullMatrixWithBams.repartition(2000).map(_.toString)
          .saveAsTextFile(finalOutput)
     } // end if tf was already defined, save
     else {
      println(s"skipping ${labelsPath}")
     }

   } // end all labels paths
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
