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
import net.akmorrow13.endive.featurizers.Kmer
import net.akmorrow13.endive.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord, ReferenceRegion}
import org.bdgenomics.adam.rdd.{ShuffleRegionJoin, GenomicRegionPartitioner}
import org.bdgenomics.adam.util.{ReferenceContigMap, ReferenceFile, TwoBitFile}
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


/**
 * Finds regions where transcription factors bind and there is no exposed chromatin 'i.e., heterochromatin'
 */
object HeteroChromatinBinding extends Serializable  {

  /**
   * A very basic dataset creation pipeline that *doesn't* featurize the data
   * but creates a csv of (Window, Label)
   *
   *  Requires chipseq labels (narrowPeak files, *.narrowPeak.gz)
   *  Requires dnase areas    (narrowPeak files, *.narrowPeak.gz)
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

  def run(sc: SparkContext, conf: EndiveConf) {

    // load reference
    val referencePath = conf.reference
    val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
    val sd = reference.sequences

    // load folders
    val dnaseFolder = conf.dnase
    val chipseqFolder = conf.chipPeaks

    val fs: FileSystem = FileSystem.get(new Configuration())

    // load dnase files
    val dnaseStatus = fs.listStatus(new Path(dnaseFolder))
    println(s"first label file: ${dnaseStatus.head.getPath.getName}")


    // load chipseq files
    val chipseqStatus = fs.listStatus(new Path(chipseqFolder))
    println(s"first dnase file: ${chipseqStatus.head.getPath.getName.split('.')(1)}")

    // for each chipseq file, load dnase and compare all regions
    for (i <- chipseqStatus) {
      val chipseqPath = i.getPath.toString
      println( chipseqPath)

      // split file format '/ChIPseq.SK-N-SH.YY1.conservative.train.narrowPeak.gz'
      val cellType = chipseqPath.split("/")(-1).split(".")(1)
      val tf =  chipseqPath.split("/")(-1).split(".")(2)

      // find DNASE match for cellType
      val dnaseFiles = dnaseStatus.map(f => f.getPath.toString).filter(_.contains(cellType))

      // assert exactly 1 cell type was found
      assert(dnaseFiles.length == 1)
      val dnasePath = dnaseFiles(0)


      // load RDDs and map out celltypes
      val chipseq: RDD[(ReferenceRegion, PeakRecord)] = Preprocess.loadPeaks(sc, chipseqPath).map(r => (r._2.region, r._2))
        .partitionBy(GenomicRegionPartitioner(sd.records.length, sd))
      val dnase: RDD[(ReferenceRegion, PeakRecord)]  = Preprocess.loadPeaks(sc,  dnasePath).map(r => (r._2.region, r._2))
        .partitionBy(GenomicRegionPartitioner(sd.records.length, sd))


      val x: RDD[(ReferenceRegion, PeakRecord)] = chipseq.zipPartitions(dnase)((c, d) => {
        // filter chipseq regions that have no dnase
        c.filter(cp => d.filter(dp => dp._1.overlaps(cp._1)).length == 0)
      })

      val count = x.count
      val totalCount = chipseq.count

      println(s"${tf}, ${cellType},${count},${totalCount}")

    }
  }
}
