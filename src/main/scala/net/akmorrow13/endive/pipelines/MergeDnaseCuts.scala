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
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.Coverage
import org.bdgenomics.adam.rdd.ADAMContext._
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import org.bdgenomics.utils.misc.Logging


object MergeDnaseCuts extends Serializable with Logging {

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

    println("STARTING DNase Processing")

    val dnasePaths = conf.dnaseLoc.split(',')
    println("processing dnase paths")
    dnasePaths.foreach(println)

    val output = conf.getFeaturizedOutput

    for (dnasePath <- dnasePaths) {
     // get cell type from dnase path
     val cellType = dnasePath.split("/").last.split('.')(2)
     println(cellType)

     val positiveFile = s"${conf.getFeaturizedOutput}${cellType}.positiveStrands.coverage.adam"
     val negativeFile = s"${conf.getFeaturizedOutput}${cellType}.negativeStrands.coverage.adam"

     var dnase = sc.loadParquetAlignments(dnasePath)
     dnase.rdd.cache()
     println(s"dnase before file filter, ${dnase.rdd.count}")

     // pick only one file
     val chosenFile = dnase.rdd.first.getAttributes
      println(s"chosen file = ${chosenFile}")
      dnase = dnase
	     .transform(rdd => rdd.repartition(1000))
	     .transform(rdd => rdd.filter(r => r.start >= 0 && r.getAttributes == chosenFile))

      println(s"dnase before file filter, ${dnase.rdd.count}")


      println("total dnase count", dnase.rdd.count)
      println("positives", dnase.rdd.filter(r => !r.getReadNegativeStrand).count)
      println("negatives", dnase.rdd.filter(r => r.getReadNegativeStrand).count)

      // group and save positive strand files
      val groupedPositives = dnase.transform(rdd => rdd.filter(r => !r.getReadNegativeStrand))
	      .toCoverage()

      groupedPositives.save(positiveFile, false)

    // group and save negative strand files
      val groupedNegatives = dnase.transform(rdd => rdd.filter(r => r.getReadNegativeStrand))
	      .toCoverage()

      groupedNegatives.save(negativeFile, false)
    }
  }
}
