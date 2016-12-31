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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.CoverageRDD
import org.bdgenomics.adam.util.{  TwoBitFile }
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
    /*
    println("STARTING DATA SET CREATION PIPELINE")

    // create new sequence with reference path
    val referencePath = conf.reference

    // load dnase path for all narrowpeak files
    val dnaseNarrowPath = conf.dnaseNarrow

    // load dnase paths for all dnase mabs
    val dnaseBamsPath = conf.dnaseBams

    // labels with sequence only. Should be null if sequences have not yet been extracted
    val labels = conf.labels

    // specifies ladder region, test region, or ladder region within cell type
    val board = 
      if (conf.hasSequences) {
        val boardSpl = labels.split("/")
        boardSpl(boardSpl.length - 2)
       } else {
        val boardSpl = labels.split('.')
        println(boardSpl, boardSpl.length - 3 )
        boardSpl(boardSpl.length - 3).split("/").last
      }

    println(s"will save to location ${conf.aggregatedSequenceOutput}test/${board}/<TESTCELLNAME>.labeledWindows")

    if (conf.getCellTypes == null) {
      println("Error: tf and cell type must be provided")
      sys.exit(-1)
    }

    val cellTypes = conf.getCellTypes.split(",").map(c => CellTypes.getEnumeration(c))

    // create sequence dictionary
    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    if (conf.hasSequences == false) {

      val sequencesAndRegions = sc.loadFeatures(conf.labels).transform(rdd => rdd.repartition(20))

      val featuresWithSequences = sequencesAndRegions.rdd.mapPartitions( part => {
        val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
        part.map { r =>
          val sequence = reference.extract(ReferenceRegion.unstranded(r))
          r.setSource(sequence) // set sequence
          r
        }
      }).repartition(500)

      val sequencePath = s"${conf.aggregatedSequenceOutput}test/${board}/sequencesAndWindows.features.adam"
       println(s"${conf.aggregatedSequenceOutput}test")
      println(s"saving to sequence path ${sequencePath}")
      sequencesAndRegions.transform(rdd => featuresWithSequences).save(sequencePath, false)
      sys.exit(0)
    }

    val windows: RDD[LabeledWindow] = {

      // loads in test regions and sequences. For this to work, sequences should be stored in getSource()
        val sequencesAndRegions = sc.loadFeatures(conf.labels).rdd

      // extract sequences
      sequencesAndRegions.map(r => {
            LabeledWindow(Window(TranscriptionFactors.Any, CellTypes.Any, // TF and CellType agnostic (just sequence)
              ReferenceRegion(r.getContigName, r.getStart, r.getEnd), r.getSource, 0), -10) // no labels for test, so -10
        }).setName("windows").cache()
    }

      println("labeled window count", windows.count)

    // iterate through all test cell types and save data
    for (testCellType <- cellTypes) {

      // now join with narrow peak dnase
      val fs: FileSystem = FileSystem.get(new Configuration())
      val dnaseNarrowStatus = fs.listStatus(new Path(dnaseNarrowPath))

      var fullMatrix: RDD[LabeledWindow] = {
        // Load DNase data of (cell type, peak record)
        val dnaseFiles = dnaseNarrowStatus.filter(r => {
          val cellType = Dataset.filterCellTypeName(r.getPath.getName.split('.')(1))
          cellType == testCellType.toString
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

      // join with dnase bams
      fullMatrix = {
        // load cuts from AlignmentREcordRDD. filter out only cells of interest
        val (positiveCoverage: CoverageRDD, negativeCoverage: CoverageRDD)
          = Preprocess.loadDnase(sc, dnaseBamsPath, testCellType)

        VectorizedDnase.joinWithDnaseCoverage(sc, sd, fullMatrix, positiveCoverage, negativeCoverage)
      }

      val saveLocation = s"${conf.aggregatedSequenceOutput}test/${board}/${testCellType.toString}.labeledWindows"
      println(s"Now saving to disk for celltyp ${testCellType.toString} at ${saveLocation}")
      fullMatrix.map(_.toString).saveAsTextFile(saveLocation)
      fullMatrix.unpersist()
    }
    */
  }

}
