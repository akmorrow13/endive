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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml


object ProcessDnaseBams extends Serializable  {

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
    val dnase = conf.dnaseBams
    val output = conf.getFeaturizedOutput

    // read all bams in file and save positive coverage
    try{
      val fs: FileSystem = FileSystem.get(new Configuration())
      val status = fs.listStatus(new Path(dnase))
      for (i <- status) {
        val file: String = i.getPath.toString
        println(s"processsing file ${file}")

        // get positive strand coverage
        val alignments = sc.loadAlignments(file).transform(_.filter(r => !r.getReadNegativeStrand))
        println("number of partitions", alignments.rdd.partitions.length)

        val coverage = alignments.toCoverage(true) // collapse coverage

        println("Now saving to disk")
        val outputFile = s"${dnase}/coverage_positive_${i.getPath.getName}.adam"
        println(outputFile)
        coverage.save(outputFile)
      }
    } catch {
      case e: Exception => println(s"Directory ${dnase} could not be loaded")
    }




  }

}
