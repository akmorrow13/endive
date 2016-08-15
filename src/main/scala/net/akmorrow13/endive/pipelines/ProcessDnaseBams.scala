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

import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.Dataset
import net.akmorrow13.endive.utils.LabeledReferenceRegionPartitioner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import org.bdgenomics.utils.misc.Logging


object ProcessDnaseBams extends Serializable with Logging {

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

    println("STARTING DNase Processing")
    val dnase = conf.dnase
    val output = conf.getFeaturizedOutput
    val referencePath = conf.reference

    // read all bams in file and save positive coverage
    try{
      val fs: FileSystem = FileSystem.get(new Configuration())
      val status = fs.listStatus(new Path(dnase)).filter(i => i.getPath.getName.endsWith(".bam"))
      for (i <- status) {
        val filePath: String = i.getPath.toString
        val fileName = i.getPath.getName
        val cellType = Dataset.filterCellTypeName(fileName.split('.')(1))
        log.info(s"processsing file ${filePath} for cell type ${cellType}")

        val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

        // get positive strand coverage and key by region, cellType
        var positiveAlignments = sc.loadAlignments(filePath).transform(_.filter(r => !r.getReadNegativeStrand))

        log.info("repartitioning positive rdd")
        val posRdd: RDD[AlignmentRecord] = positiveAlignments.rdd
                      .filter(r => r.getContigName != null)
                      .keyBy(r => (r.getContigName, cellType))
                      .partitionBy(new LabeledReferenceRegionPartitioner(sd, Dataset.cellTypes.toVector))
                      .map(r => r._2)

        positiveAlignments = positiveAlignments.transform(r => posRdd)
        val positiveCoverage = positiveAlignments.toCoverage(true) // collapse coverage

        log.info("Now saving positive coverage to disk")
        var outputFile = s"${dnase}/coverage/positive/${fileName}.adam"
        log.info(outputFile)
        positiveCoverage.save(outputFile)


        // Calculate negative coverage
        var negativeAlignments = sc.loadAlignments(filePath).transform(_.filter(r => r.getReadNegativeStrand))
        log.info("repartitioning negative rdd")
        val negRdd: RDD[AlignmentRecord] = negativeAlignments.rdd
          .filter(r => r.getContigName != null)
          .keyBy(r => (r.getContigName, cellType))
          .partitionBy(new LabeledReferenceRegionPartitioner(sd, Dataset.cellTypes.toVector))
          .map(r => r._2)

        negativeAlignments = negativeAlignments.transform(r => negRdd)
        val negativeCoverage = negativeAlignments.toCoverage(true) // collapse coverage

        log.info("Now saving negative coverage to disk")
        outputFile = s"${dnase}/coverage/negative/${fileName}.adam"
        log.info(outputFile)
        negativeCoverage.save(outputFile)

      }
    } catch {
      case e: Exception => println(s"Directory ${dnase} could not be loaded")
    }




  }

}
