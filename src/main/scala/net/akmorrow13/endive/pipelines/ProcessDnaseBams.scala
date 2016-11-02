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
import net.akmorrow13.endive.processing.{Cut, Dataset}
import net.akmorrow13.endive.processing.{Cut, Dataset}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ReferencePartitioner
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

    val fs: FileSystem = FileSystem.get(new Configuration())
    val positiveFolder = s"${output}/"
    val saved = fs.listStatus(new Path(positiveFolder)).map(_.getPath.toString)

    // read all bams in file and save positive coverage
    val status: Array[FileStatus] = fs.listStatus(new Path(dnase)).filter(i => i.getPath.getName.endsWith(".bam"))
    val cellTypeFiles = status.map(f => (Dataset.filterCellTypeName(f.getPath.getName.split('.')(1)), f)).groupBy(_._1)

    // get sequence dicionary
    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    // TODO: check if group exists
    for (grp <- cellTypeFiles) {
      val cellType = grp._1
      println(s"processing Dnase for celltype ${cellType}")

      val totalCuts: RDD[Cut] = sc.emptyRDD[Cut]
      val outputLocation = s"${output}/DNASE.${cellType}.adam"
      totalCuts.cache()

      if (!saved.contains(outputLocation)) {

        for (i <- grp._2.map(_._2)) {
          val filePath: String = i.getPath.toString
          val fileName = i.getPath.getName
          println(s"processing file ${filePath}")

          // get positive strand coverage and key by region, cellType
          val alignments = sc.loadAlignments(filePath)
          alignments.rdd.cache

          val cuts: RDD[Cut] = alignments.rdd
            .filter(r => r.getContigName != null)
            .map(r => Cut(ReferenceRegion(r), fileName, r.getReadName, r.getReadNegativeStrand))

          totalCuts.union(cuts)
          alignments.rdd.unpersist(false)
        }

        log.info(s"Now saving dnase cuts for ${cellType} to disk")
        // TODO: save cuts for celltype
        totalCuts.map(_.toString).saveAsTextFile(output)
        totalCuts.unpersist(true)
      } else {
        println(s"dnase for ${cellType} exists. skipping")
      }

    }



  }

}
