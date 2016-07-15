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

import net.akmorrow13.endive.processing.Sequence
import net.akmorrow13.endive.EndiveConf
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

object StrawmanPipeline extends Serializable  {

  /**
   * A very basic pipeline that *doesn't* featurize the data
   * simple regresses the raw sequence with the labels for the sequence
   *
   * HUGE CAVEATS
   * Trains a separate model for each TF type
   * Ignores cell type information
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


    println("STARTING STRAWMAN PIPELINE")
    // create new sequence with reference path
    val referencePath = conf.reference
    val reference = Sequence(referencePath, sc)

    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val train: RDD[(ReferenceRegion, Double)] = loadTsv(sc, labelsPath)

    // extract sequences from reference over training regions
    val sequences: RDD[(ReferenceRegion, String)] = reference.extractSequences(train.map(_._1))

    println("NUM SEQUENCES " + sequences.count())
  }

  def loadTsv(sc: SparkContext, filePath: String): RDD[(ReferenceRegion, Double)] = {
    val rdd = sc.textFile(filePath).filter(!_.contains("start"))
    rdd.map(line=> {
      val parts = line.split("\t")
      /* TODO: ignoring cell types for now */
      val label = parts.slice(3,parts.size).map(extractLabel(_)).reduceLeft(_ max _)
      (ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong), label)
    })
  }

  def extractLabel(s: String): Double = {
    s match {
      case "A" => -1.0 // ambiguous
      case "U" => 0.0  // unbound
      case "B" => 1.0  // bound
      case _ => throw new IllegalArgumentException(s"Illegal label ${s}")
    }
  }
}
