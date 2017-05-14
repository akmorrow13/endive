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

import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.formats.avro.Feature
import java.io.File
import net.akmorrow13.endive.EndiveConf
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.bdgenomics.adam.rdd.InnerShuffleRegionJoinAndGroupByLeft
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{Coverage, SequenceDictionary, ReferenceRegion}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{TwoBitFile}
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object FindNovel extends Serializable  {

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
      val conf = new SparkConf().setAppName("ENDIVE:SingleTFDatasetCreationPipeline")
      conf.setIfMissing("spark.master", "local[4]")
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }


  def run(sc: SparkContext, conf: EndiveConf) {


    val genes = sc.loadGtf(conf.genes).rdd.map(r => ReferenceRegion.unstranded(r)).collect
    println("genes count", genes.length)

    //coverage, beds and gtf file

    val fileNames = Preprocess.getFileNamesFromDirectory(sc, conf.getFeaturesOutput)

    val peaks = fileNames.map(file => {
      // set cell type somewhere
      val cellType = file.split("/").last.split('.')(1)
      val x = sc.loadFeatures(file).transform(rdd => rdd.map(r => {
        r.setName(cellType)
        r
      }))
      x
    }).reduce((r1, r2) => r1.transform(rdd => rdd.union(r2.rdd)))

    val coverageFiles = Preprocess.getFileNamesFromDirectory(sc, conf.getDnaseLoc)
    val coverage = coverageFiles.map(file => {
      // set cell type somewhere
      val cellType = file.split("/").last.split('.')(1)
      sc.loadFeatures(file).transform(rdd => rdd.map(r => {
        r.setName(cellType)
        r
      }))
    }).reduce((r1, r2) => r1.transform(rdd => rdd.union(r2.rdd)))

    val coveragePeaks = peaks.transform(rdd => rdd.map(r => {
      r.setScore(1.0)
      r
    })).toCoverage

    val bpPerBin = 1000

    val tmp = coveragePeaks.flatten.rdd.keyBy(r => {
          // key coverage by binning start site mod bpPerbin
          // subtract region.start to shift mod to start of ReferenceRegion
          val start = r.start - (r.start % bpPerBin)
          ReferenceRegion(r.contigName, start, start + bpPerBin)
        }).mapValues(r => r.count)
        .reduceByKey(_ + _)
        .map(r => {
          // compute total coverage in bin
		
    	  Feature.newBuilder()
    	  .setContigName(r._1.referenceName)
          .setStart(r._1.start)
     	  .setEnd(r._1.end)
    	  .setScore(r._2)
      	  .build()    
    })
    val aggregatedCoveragePeaks = FeatureRDD(tmp, coverage.sequences).toCoverage

    val maxPeaks = aggregatedCoveragePeaks.transform(rdd => rdd.filter(_.count > 1))
    println("maxPeaks", maxPeaks.rdd.count)

    val genesB = sc.broadcast(genes)
    // run join code here: right side should be peaks
    val joined: RDD[(Coverage, Double)] = InnerShuffleRegionJoinAndGroupByLeft[Coverage, Coverage](coverage.sequences, 1000000, sc)
      .partitionAndJoin(maxPeaks.rdd.keyBy(r => ReferenceRegion(r)), coverage.toCoverage.rdd.keyBy(r => ReferenceRegion(r)))
      .map(r => (r._1, r._2.toArray.map(_.count).sum))
      .filter(r => {
        !genesB.value.filter(g => {
          ReferenceRegion(r._1).distance(g).getOrElse(-100000000L) < 1000
        }).isEmpty
      }).cache()


    println("after join and filtering by genes", joined.count, joined.first)

    // Query 1: Find sites where binding across cell types is most consistent
    val populatedRegions = joined.sortBy(_._1.count, ascending = false).take(10)
    println("most populated peak regions")
    populatedRegions.foreach(println)

    // filter out regions with ANY coverage
    val noCoverage = joined.filter(_._2 < 100)
    println("areas with no coverage", noCoverage.count)

    noCoverage.sortBy(_._1.count, ascending = false).take(10).foreach(println)

    // Query 3: Find regions where all celltypes have very high coverage (and overlap gene?)
    // filter out regions with ANY coverage
    val highCoverage = joined.sortBy(_._2, ascending=false)
    println("areas with high coverage", highCoverage.count)

    highCoverage.take(10).foreach(println)


  }


}
