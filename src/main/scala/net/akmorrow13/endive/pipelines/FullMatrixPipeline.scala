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
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.util.{ReferenceContigMap, ReferenceFile, TwoBitFile}
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object FullMatrixPipeline extends Serializable  {

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
      val conf = new SparkConf().setAppName("ENDIVE")
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {

    println("STARTING DATA SET CREATION PIPELINE")
    val stride = 50
    val windowSize = 200 // defined by competition
    // create new sequence with reference path
    val referencePath = conf.reference
    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val geneReference = conf.genes

    // RDD of (tf name, celltype, region, score)
    val labels: RDD[(String, String, ReferenceRegion, Int)] = Preprocess.loadLabelFolder(sc, labelsPath)

    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] =
          DatasetCreationPipeline.extractSequencesAndLabels(referencePath, labels)

    // Load DNase data of (cell type, peak record)
    val dnaseRDD: RDD[(String, PeakRecord)] = Preprocess.loadPeakFolder(sc, conf.dnase)


    val dnase = new DNase(windowSize, stride, dnaseRDD)

    val dnaseMapped = dnase.joinWithSequences(sequences)
    dnaseMapped.map(_.toString).saveAsTextFile(conf.featureLoc)

    // Load RNA seq data of (cell type, rna transcript)
//    val rnaLoader = new RNAseq(geneReference, sc)
//    val rnaseq: RDD[(String, RNARecord)] = rnaLoader.loadRNAFolder(sc, conf.rnaseq)
//
//    println("RNA seq")
//    print(rnaseq.count)


    println("DNase")
//    println(dnase.count)

//    println("saving rnaseq")
//    rnaseq.map(_.toString).saveAsTextFile(conf.rnaseqLoc)

    println("saving dnase")
//    dnase.map(_.toString).saveAsTextFile(conf.dnaseLoc)



    // combine all data sources together


//    println("Now matching labels with reference genome")
//    sequences.count()

//    println("Now saving to disk")
//    sequences.map(_.toString).saveAsTextFile(conf.windowLoc)
  }

}
