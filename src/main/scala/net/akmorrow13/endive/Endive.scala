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
package net.akmorrow13.endive

import net.akmorrow13.endive.featurizers.Kmer
import net.akmorrow13.endive.processing.{Preprocess, Sequence}
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
import pipelines.Logging

object Endive extends Serializable with Logging {
  val commandName = "endive"
  val commandDescription = "computational methods for sequences and epigenomic datasets"
  /**
   * The actual driver receives its configuration parameters from spark-submit usually.
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
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
      // NOTE: ONLY APPLICABLE IF YOU CAN DONE COPY-DIR
      conf.remove("spark.jars")
      conf.setIfMissing("spark.master", "local[16]")
      conf.set("spark.driver.maxResultSize", "0")
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {

    // create new sequence with reference path
    val referencePath = conf.reference
    val reference = Sequence(referencePath, sc)
    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val train: RDD[(ReferenceRegion, Double)] = Preprocess.loadLabels(sc, labelsPath)

    // extract sequences from reference over training regions
    val sequences: RDD[(ReferenceRegion, String)] = reference.extractSequences(train.map(_._1))

    // extract kmer counts from sequences
    val kmers: RDD[LabeledPoint] = Kmer.extractKmers(sequences, conf.kmerLength).zip(train.map(_._2))
                                                .map(r => LabeledPoint(r._2, r._1))
  }


  /**
   * Loads bed file over optional region
   * @param sc SparkContext
   * @param featurePath Feature path to load
   * @param region Optional region to load features from
   * @return RDD of Features
   */
  def loadFeatures(sc: SparkContext, featurePath: String, region: Option[ReferenceRegion] = None): RDD[Feature] = {
    val predicate =  Some((BinaryColumn("contig.contigName") === region.get.referenceName))

      if (featurePath.endsWith(".adam")) sc.loadParquetFeatures(featurePath, predicate)
    else if (featurePath.toLowerCase.endsWith("bed")) sc.loadFeatures(featurePath)
    else throw new Exception("File type not supported")
  }
}

