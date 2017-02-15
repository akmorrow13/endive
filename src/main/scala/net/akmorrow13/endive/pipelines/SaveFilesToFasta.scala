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

import java.io._
import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing.{Chromosomes, CellTypes, TranscriptionFactors}
import net.akmorrow13.endive.utils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.bdgenomics.adam.rdd.contig.NucleotideContigFragmentRDD
import org.bdgenomics.adam.rdd.feature.CoverageRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{TwoBitFile}
import org.bdgenomics.formats.avro.{Strand, Contig, NucleotideContigFragment}
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.BlockLeastSquaresEstimator
import net.jafama.FastMath
import org.apache.commons.math3.random.MersenneTwister
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import com.github.fommil.netlib.BLAS

object SaveFilesToFasta extends Serializable {

  /**
   * A very basic dataset creation pipeline for sequence data that *doesn't* featurize the data
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
      val conf = new SparkConf().setAppName(configtext)
      conf.setIfMissing("spark.master", "local[4]")
      val sc = new SparkContext(conf)
      val blasVersion = BLAS.getInstance().getClass().getName()
      println(s"Currently used version of blas is ${blasVersion}")
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {
    println("RUN SOLVER ONLY PIPELINE")

    // generate headers
    val headers = sc.textFile(conf.deepSeaDataPath + "headers.csv").first().split(",")
    val headerTfs: Array[String] = headers.map(r => r.split('|')).map(r => r(1))
    println(s"headerTfs: ${headerTfs.head} ${headerTfs.length}")

    var indexTf = headers.zipWithIndex.filter(r => r._1.contains(conf.tfs)).head
    indexTf = (indexTf._1.filter(_ != '|'), indexTf._2)

    val eval = readSequences(s"${conf.windowLoc}deepsea_eval_reg_seq", sc)
      .map(r => (r._1,r._2,r._3(indexTf._2)))
      .repartition(1)
    val test = readSequences(s"${conf.windowLoc}deepsea_eval_reg_seq", sc)
      .map(r => (r._1,r._2,r._3(indexTf._2)))
      .repartition(1)

    val sequences = DatasetCreationPipeline.getSequenceDictionary(conf.reference)

    val evalLoc = s"${conf.featuresOutput}${indexTf._1}_eval.fasta"
    val testLoc = s"${conf.featuresOutput}${indexTf._1}_test.fasta"
    val evalLabelLoc = s"${conf.featuresOutput}${indexTf._1}_eval_labels.csv"
    val testLabelLoc = s"${conf.featuresOutput}${indexTf._1}_test_labels.csv"

    // save eval
    saveToFasta(sc, sequences, eval, evalLoc, evalLabelLoc)

    // save test
    saveToFasta(sc, sequences, test, testLoc, testLabelLoc)

  }

  def readSequences(filePath: String, sc: SparkContext): RDD[(ReferenceRegion, String, Array[Int])] = {
    sc.textFile(filePath)
      .map(r => {
        val arr = r.split(",")
        val region = ReferenceRegion(arr(0),arr(1).toLong, arr(2).toLong, Strand.valueOf(arr(3)))
        val sequences = arr(4)
        val labels = arr.slice(5, arr.length).map(_.toInt)
        (region, sequences, labels)
      })
  }

  def saveToFasta(sc: SparkContext,
                  sequences: SequenceDictionary,
                  rdd: RDD[(ReferenceRegion, String, Int)],
                  filePath: String,
                  labelPath: String) = {

    // save fasta
    new NucleotideContigFragmentRDD(
      rdd.map(r => {
        NucleotideContigFragment.newBuilder()
          .setFragmentSequence(r._2)
            .setDescription(s"${r._1.toString}")
            .setContig(Contig.newBuilder().setContigName(r._1.referenceName).build)
            .setFragmentLength(r._1.length)
            .build()
      }), sequences).saveAsFasta(filePath)

    // save labels
    rdd.map(r => r._3).saveAsTextFile(labelPath)
  }
}