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
import net.akmorrow13.endive.processing.Sequence
import net.akmorrow13.endive.utils._
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord, ReferenceRegion}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.GenomicPositionPartitioner
import org.bdgenomics.adam.util.{ReferenceContigMap, ReferenceFile, TwoBitFile}
import org.bdgenomics.formats.avro._
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object DatasetCreationPipeline extends Serializable  {

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
    // create new sequence with reference path
    val referencePath = conf.reference
    if (referencePath == null)
      throw new Exception("referencepath not defined")
    val genes = conf.genes
    if (genes == null)
      throw new Exception("gene path not defined")
    val aggregatedSequenceOutput = conf.aggregatedSequenceOutput
    if (aggregatedSequenceOutput == null)
      throw new Exception("aggregatedSequenceOutput not defined")
    val labelsPath = conf.labels
    if (labelsPath == null)
      throw new Exception("chipseq labels not defined")
    val dnasePath = conf.dnase
    if (dnasePath == null)
      throw new Exception("dnasePath not defined")
    val rnaseqPath = conf.rnaseq
    if (rnaseqPath == null)
      throw new Exception("rnaseqPath not defined")

    // challenge parameters
    val windowSize = 200
    val stride = 50

    val train: RDD[(String, String, ReferenceRegion, Int)] = Preprocess.loadLabelFolder(sc, labelsPath)
        .cache()

    println("First reading labels")
    train.count()

    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] = extractSequencesAndLabels(referencePath, train).cache()

    // Load DNase data of (cell type, peak record)
    val dnase: RDD[(String, PeakRecord)] = Preprocess.loadPeakFolder(sc, dnasePath)
      .cache()

//    // load rnase data
//    val rnaLoader = new RNAseq(genes, sc)
//    val rnaseq: RDD[(String, RNARecord)] = rnaLoader.loadRNAFolder(sc, rnaseqPath)
//      .cache()

    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    val cellTypeInfo = new CellTypeSpecific(windowSize, stride, dnase, sc.emptyRDD[(String, RNARecord)], sd)

    val fullMatrix: RDD[LabeledWindow] = cellTypeInfo.joinWithDNase(sequences)

    // save data
    fullMatrix.map(_.toString).saveAsTextFile(aggregatedSequenceOutput)

    println("Now matching labels with reference genome")
    sequences.count()

  }

  def getSequenceDictionary(referencePath: String): SequenceDictionary = {
    val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
    new SequenceDictionary(reference.seqRecords.toVector.map(r => SequenceRecord(r._1, r._2.dnaSize)))
  }


  def extractSequencesAndLabels(referencePath: String, regionsAndLabels: RDD[(String, String, ReferenceRegion, Int)]): RDD[LabeledWindow]  = {
    /* TODO: This is a kludge that relies that the master + slaves share NFS
     * but the correct thing to do is to use scp/nfs to distribute the sequence data
     * across the cluster
     */

    regionsAndLabels.mapPartitions { part =>
        val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
        part.map { r =>
          val startIdx = r._3.start
          val endIdx = r._3.end
          val sequence = reference.extract(r._3)
          val label = r._4
          val win = Window(r._1, r._2, r._3, sequence, List(), List())
          LabeledWindow(win, label)
        }
      }
  }

}
