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
package net.akmorrow13.endive.processing

import java.io.{InputStreamReader, BufferedReader, File}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred._
import org.apache.hadoop.util._

object Preprocess {

  /**
   * Loads tsv file
   * @param sc SparkContext
   * @param filePath tsv filepath to load
   * @param headerTag tags in first row, if any, that should be excluded from load
   * @return RDD of rows from tsv file
   */
  def loadTsv(sc: SparkContext, filePath: String, headerTag: String): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath).filter(r => !r.contains(headerTag))
    println(s"Loaded file ${filePath} with ${rdd.count} records")
    rdd.map( line => {
      line.split("\t")
    })
  }

  /**
   * Loads labels from all a chipseq label file
   * flatmaps all cell types into an individual datapoint
   * @param sc
   * @param filePath tsv file of chipseq labels
   * @return parsed files of (tf, cell type, region, score)
   */
  def loadLabels(sc: SparkContext, filePath: String): RDD[(String, String, ReferenceRegion, Int)] = {
    assert(filePath.endsWith("tsv") || filePath.endsWith("tsv.gz"))
    val headerTag = "start"
    // parse header for cell types
    val cellTypes = sc.textFile(filePath).filter(r => r.contains(headerTag)).first().split("\t").drop(3)
    val file = filePath.split("/").last
    // parse file name for tf
    val tf = file.split('.')(0)
    val rdd = loadTsv(sc, filePath, headerTag)
    rdd.flatMap(parts => {
      cellTypes.zipWithIndex.map( cellType => {
        (tf, cellType._1, ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong), extractLabel(parts(3 + cellType._2)))
      })
    })
  }

  def loadLabelFolder(sc: SparkContext, folder: String): RDD[(String, String, ReferenceRegion, Int)] = {
    var data: RDD[(String, String, ReferenceRegion, Int)] = sc.emptyRDD[(String, String, ReferenceRegion, Int)]
    val d = new File(folder)
    if (sc.isLocal) {
      if (d.exists && d.isDirectory) {
        val files = d.listFiles.filter(_.isFile).toList
        files.map(f => {
          data = data.union(loadLabels(sc, f.getPath))
        })
      } else {
        throw new Exception(s"${folder} is not a valid directory for peaks")
      }
    } else {
    try{
      val fs: FileSystem = FileSystem.get(new Configuration())
      val status = fs.listStatus(new Path(folder))
      for (i <- status) {
        val file: String = i.getPath.getName
        data = data.union(loadLabels(sc, file))
      }
    } catch {
      case e: Exception => println(s"Directory ${folder} could not be loaded")
    }
  }
    data
  }

  /**
   * Used to load in gene reference file
   * should be a gtf file
   * @param sc
   * @param filePath
   * @return
   */
  def loadTranscripts(sc: SparkContext, filePath: String): RDD[Transcript] = {
    // extract cell type

    val rdd = loadTsv(sc, filePath, "##")
    val transcripts = mapAttributes(
      rdd
      .filter(parts => parts(2) == "transcript"))

    transcripts.map(r => Transcript(r._2, r._3, r._1))

  }

  /**
   * Maps attributes in gtf file for gtf files delimited with ';'
   * @param rdd
   * @return
   */
  private def mapAttributes(rdd: RDD[Array[String]]): RDD[(ReferenceRegion, String, String, String)] = {
    rdd.map(parts => {
      val attrs =
        parts(8).split("; ")
          .map(s => {
            val eqIdx = s.indexOf(" ")
            (s.take(eqIdx), s.drop(eqIdx + 1).replaceAll("\"", ""))
          }).toMap
      (ReferenceRegion(parts(0), parts(3).toLong, parts(4).toLong),
        attrs.get("gene_id").get, // gene Id
        attrs.get("transcript_id").get,  // transcript id
        parts(2)) // record type
    })
  }



  /**
   * Loads narrowPeak files, which are tab delimited peak files
   * see https://genome.ucsc.edu/FAQ/FAQformat.html
   *
   * @param sc
   * @param filePath
   */
  def loadPeaks(sc: SparkContext, filePath: String): RDD[(String, PeakRecord)] = {
    val cellType = filePath.split("/").last.split('.')(1)
    val rdd = loadTsv(sc, filePath, "any")
    rdd.map(parts => {
      val region = ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong)
      val l = parts.drop(3).toList.filter(r => r != ".")
      val score = l(0).toInt
      val signalValue = l(1).toDouble
      val pValue = l(2).toDouble
      val qValue = l(3).toDouble
      val peak = l(4).toDouble
      (cellType, PeakRecord(region, score, signalValue, pValue, qValue, peak))
    })
  }

  def loadPeakFolder(sc: SparkContext, folder: String): RDD[(String, PeakRecord)] = {

    var data: RDD[(String, PeakRecord)] = sc.emptyRDD[(String, PeakRecord)]
    if (sc.isLocal) {
      val d = new File(folder)
      if (d.exists && d.isDirectory) {
        val files = d.listFiles.filter(_.isFile).toList
        files.map(f => {
          data = data.union(loadPeaks(sc, f.getPath))
        })
      } else {
        throw new Exception(s"${folder} is not a valid directory for peaks")
      }
    } else {
      try{
        val fs: FileSystem = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(folder))
        for (i <- status) {
        val file: String = i.getPath.getName
        data = data.union(loadPeaks(sc, file))
      }
      } catch {
        case e: Exception => println(s"Directory ${folder} could not be loaded")
      }
    }
    data
  }

    def extractLabel(s: String): Int = {
    s match {
      case "A" => -1 // ambiguous
      case "U" => 0  // unbound
      case "B" => 1  // bound
      case _ => throw new IllegalArgumentException(s"Illegal label ${s}")
    }
  }

}

/**
 *
 * @param geneId
 * @param transcriptId
 * @param length
 * @param effective_length
 * @param expected_count
 * @param TPM: transcripts per million
 * @param FPKM: fragments per kilobase of exon per million reads mapped
 */
case class RNARecord(region: ReferenceRegion, geneId: String, transcriptId: String, length: Double, effective_length: Double,	expected_count: Double,	TPM: Double,	FPKM: Double)

/**
 *
 * @param score Indicates how dark the peak will be displayed in the browser (0-1000). If all scores were '0' when the data were submitted to the DCC, the DCC assigned scores 1-1000 based on signal value. Ideally the average signalValue per base spread is between 100-1000.
strand - +/- to denote strand or orientation (whenever applicable). Use '.' if no orientation is assigned.
 * @param signalValue Measurement of overall (usually, average) enrichment for the region.
 * @param pValue Measurement of statistical significance (-log10). Use -1 if no pValue is assigned.
 * @param qValue Measurement of statistical significance using false discovery rate (-log10). Use -1 if no qValue is assigned.
 * @param peak Point-source called for this peak; 0-based offset from chromStart. Use -1 if no point-source called.
 */
case class PeakRecord(region: ReferenceRegion, score: Int, signalValue: Double, pValue: Double, qValue: Double, peak: Double) {
  override
  def toString: String = {
    s"${region.referenceName},${region.start},${region.end},${score},${signalValue},${pValue},${qValue},${peak}"
  }
}

object PeakRecord {
  def fromString(str:String): PeakRecord = {
    val parts = str.split(",")
    val region = ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong)
    PeakRecord(region, parts(3).toInt, parts(4).toDouble, parts(5).toDouble, parts(6).toDouble, parts(7).toDouble)
  }
}
case class Transcript(geneId: String, transcriptId: String, region: ReferenceRegion)
