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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{ReferenceRegion, Coverage}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.{Strand, Feature}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import org.bdgenomics.utils.misc.Logging

object MergeLabelsAndSequences extends Serializable with Logging {

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
    val labelBed = conf.getLabels       // regions (only has POSITIVE strands)
    val sequenceFile = conf.getSequenceLoc // sequence and labels
    val featuresOutput = conf.getFeaturesOutput

    // expand out labels for forward and reverse strand
    val l = sc.loadFeatures(labelBed)

    val labels = l.rdd.flatMap(r => {
          val start = r.getStart - 400 // expand region size out to 1000
          val end = r.getEnd + 400
          val forward = Feature.newBuilder(r)
            .setStrand(Strand.FORWARD)
            .setStart(start)
            .setEnd(end)
            .build()
          val reverse = Feature.newBuilder(forward)
            .setStrand(Strand.REVERSE)
            .build()
          Iterable(forward, reverse)
        }).sortBy(f => ReferenceRegion(f)).zipWithIndex().map(r => (r._2, r._1))


    val sequences = sc.textFile(sequenceFile).zipWithIndex().map(r => (r._2, r._1))

    val labelsCount = labels.count()
    val sequenceCount = sequences.count()
    require(labelsCount == sequenceCount, s"required that labelsCount (${labelsCount}) ==" +
      s" sequenceCount (${sequenceCount})")

    // join sequences and labels and save

    val zipped = labels.join(sequences, 1).sortBy(_._1)


    val strings = zipped.map(r =>
      s"${r._2._1.getContigName},${r._2._1.getStart},${r._2._1.getEnd},${r._2._1.getStrand},${r._2._2}")

//    strings.saveAsTextFile(featuresOutput)

    // save as bed file for multiple cell types for train
    if (!conf.testBoard) {
      // get array of positions with relevant cell types
      // GM12878,h1-hESC,K562,HCT-116
      val cellTypes = Array(("GM12878",53),("H1-hESC",54),("K562",61),("HCT-116",80))

      for (cellType <- cellTypes) {
        val rdd = zipped.map(r => {
          Feature.newBuilder(r._2._1)
              .setScore(r._2._2.split(',')(cellType._2+1).toInt.toDouble)
              .build()
        })
        new FeatureRDD(rdd, l.sequences)
//          .saveAsBed(s"featuresOutput_${cellType._1}.bed")
      }
    }



  }
}
