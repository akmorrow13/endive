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
import breeze.linalg.DenseVector
import evaluation.BinaryClassifierEvaluator
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.Motif
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.utils._
import net.akmorrow13.endive.processing.Dataset
import nodes.learning.LogisticRegressionEstimator
import nodes.util.ClassLabelIndicatorsFromIntLabels

import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary, ReferenceRegion}
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object Deepbind extends Serializable {

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
      val yaml = new Yaml(new Constructor(classOf[EndiveConf]))
      val appConfig = yaml.load(configtext).asInstanceOf[EndiveConf]
      EndiveConf.validate(appConfig)
      val conf = new SparkConf().setAppName("ENDIVE")
      conf.setIfMissing("spark.master", "local[4]")
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {
    println("STARTING DEEPBIND PIPELINE")

    // challenge parameters
    val windowSize = 200
    val stride = 50

    if (conf.aggregatedSequenceOutput == null)
      throw new Exception("output for deepbind scores not defined")
    val aggregatedSequenceOutput = conf.aggregatedSequenceOutput

    if (conf.reference == null)
      throw new Exception("reference not defined")
    val referencePath = conf.reference

    if (conf.deepbindPath == null)
      throw new Exception("deepbind path not defined")
    val deepbindPath = conf.deepbindPath

    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    Dataset.chrs.foreach(chr => {
      // divy up chr into 50 partitions
      val rec: Option[SequenceRecord] =
        sd.apply(chr)

      if (rec.isDefined) {
        val chrLength = rec.get.length
        val region = ReferenceRegion(chr, 0, chrLength)
        // map to windows
        val regions: RDD[ReferenceRegion] = sc.parallelize(CellTypeSpecific.unmergeRegions(region, windowSize, stride))
                  .repartition(100)

        val sequences = extractSequences(referencePath, regions).cache()
        println(s"extracted sequences for ${chr}, ${sequences.count}")

        val motifFinder = new Motif(sc, deepbindPath)
        val scores: RDD[Array[Double]] = motifFinder.getDeepBindScoresPerPartition(sequences.map(_._2), Dataset.tfs).cache()
        println("completed deepbind scoring:", scores.count)

        val finalResults: RDD[String] = regions.zip(scores)
            .map( r=> (s"${r._1.start}:${r._1.end}:${r._2.mkString(",")}"))
        println(s"completed serialization of scores and regions, ${finalResults}")

        // save scores to chr output + chr
        val fileLocation = s"${aggregatedSequenceOutput}_${chr}"
        println(s"saving to file location:${fileLocation}")
        finalResults.saveAsTextFile(fileLocation)

      } else throw new Exception(s"${chr} not found in sd")

    })


  }

  def extractSequences(referencePath: String, regions: RDD[ReferenceRegion]): RDD[(ReferenceRegion, String)]  = {
    /* TODO: This is a kludge that relies that the master + slaves share NFS
     * but the correct thing to do is to use scp/nfs to distribute the sequence data
     * across the cluster
     */

    regions.mapPartitions { part =>
      val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))
      part.map { r =>
        (r, reference.extract(r))
      }
    }
  }
}
