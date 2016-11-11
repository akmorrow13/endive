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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{SequenceDictionary, SequenceRecord, ReferenceRegion}
import org.bdgenomics.adam.rdd.{ShuffleRegionJoin, GenomicRegionPartitioner}
import org.bdgenomics.adam.util.{ReferenceContigMap, ReferenceFile, TwoBitFile}
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._
import scala.reflect.ClassTag
import scala.annotation.tailrec


/**
 * Finds regions where transcription factors bind and there is no exposed chromatin 'i.e., heterochromatin'
 */
object HeterochromatinBinding extends Serializable  {

  /**
   * A very basic dataset creation pipeline that *doesn't* featurize the data
   * but creates a csv of (Window, Label)
   *
   *  Requires chipseq labels (narrowPeak files, *.narrowPeak.gz)
   *  Requires dnase areas    (narrowPeak files, *.narrowPeak.gz)
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

  /**
   * Tail recursion for merging adjacent ReferenceRegions with the same value.
   *
   * @param iter partition iterator of ReferenceRegion and coverage values.
   * @param lastCoverage the last coverage from a sorted Iterator that has been considered to merge.
   * @param condensed Condensed iterator of iter with adjacent regions with the same value merged.
   * @return merged tuples of adjacent ReferenceRegions and coverage.
   */
  @tailrec private def collapse[T: ClassTag](iter: Iterator[(ReferenceRegion, T)],
                                last: (ReferenceRegion, T),
                                condensed: List[(ReferenceRegion, T)]): Iterator[(ReferenceRegion, T)] = {
    if (!iter.hasNext) {
      // if lastCoverage has not yet been added, add to condensed
      val nextCondensed =
        if (condensed.map(r => r._1).filter(_.overlaps(last._1)).isEmpty) {
          last :: condensed
        } else {
          condensed
        }
      nextCondensed.toIterator
    } else {
      val cov = iter.next
      val rr = cov._1
      val lastRegion = last._1
      val (nextCoverage, nextCondensed) =
        if (rr.isAdjacent(lastRegion) || rr.overlaps(lastRegion)) {
          ((rr.merge(lastRegion), cov._2), condensed)
        } else {
          (cov, last :: condensed)
        }
      collapse(iter, nextCoverage, nextCondensed)
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {

    // load reference
    val referencePath = conf.reference
    val reference = new TwoBitFile(new LocalFileByteAccess(new File(referencePath)))

    val sd = reference.sequences

    // load folders
    val dnaseFolder = conf.dnase
    val chipseqFolder = conf.chipPeaks

    val fs: FileSystem = FileSystem.get(new Configuration())

    // load dnase files
    val dnaseStatus = fs.listStatus(new Path(dnaseFolder))
    println(s"first label file: ${dnaseStatus.head.getPath.getName}")


    // load chipseq files
    val chipseqStatus = fs.listStatus(new Path(chipseqFolder))
    println(s"first dnase celltype: ${chipseqStatus.head.getPath.getName.split('.')(0)}")

    // for each chipseq file, load dnase and compare all regions
    for (i <- chipseqStatus) {
      val chipseqPath = i.getPath.toString
      println(chipseqPath)

      // split file format '/ChIPseq.SK-N-SH.YY1.conservative.train.narrowPeak.gz'
      val tf = chipseqPath.split("/").last.split('.')(0)


      // load RDDs and map out celltypes
      val (chipseqRDD, celltypes): Tuple2[RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)], Array[CellTypes.Value]] = 
	Preprocess.loadLabels(sc, chipseqPath)

     for (ct <- celltypes) {
        val cellType = ct.toString

	var chipseq = chipseqRDD.filter(r => r._2 == ct && r._4 > 0)
	  .map(r => (r._3, r))
	  .partitionBy(GenomicRegionPartitioner(sd.records.length, sd))

	// collapse chipseq for neighboring labels
	chipseq = chipseq
      	.mapPartitions(iter => {
 	    if (iter.hasNext) {
        	val first = iter.next
          	collapse(iter, first, List.empty)
            } else iter
	})

        // find DNASE match for cellType
        val dnaseFiles = dnaseStatus.map(f => f.getPath.toString).filter(r => {
	  ct ==  CellTypes.getEnumeration(r.split("/").last.split('.')(1))
	})

        // assert exactly 1 cell type was found
        assert(dnaseFiles.length == 1)
        val dnasePath = dnaseFiles(0)

	// val chipseq: RDD[(ReferenceRegion, PeakRecord)] = Preprocess.loadPeaks(sc, chipseqPath).map(r => (r._2.region, r._2))
	//   .partitionBy(GenomicRegionPartitioner(sd.records.length, sd))
     
        val dnase: RDD[(ReferenceRegion, PeakRecord)]  = Preprocess.loadPeaks(sc,  dnasePath).map(r => (r._2.region, r._2))
          .partitionBy(GenomicRegionPartitioner(sd.records.length, sd))


        val x: RDD[ReferenceRegion] = chipseq.zipPartitions(dnase)((c, d) => {
          // filter chipseq regions that have no dnase
	  val cl = c.toList
	  val dl = d.toList
	  // filter out chipseq labels that have dnase overlap
	  cl.filter(cp => dl.filter(dp => dp._1.overlaps(cp._1)).length == 0).map(_._1).toIterator
        })
        val count = x.count
        val totalCount = chipseq.count
	val frac = count.toFloat/totalCount
        println(s"${tf}, ${cellType},${count},${totalCount},${frac}")

      }

    }
  }
}
