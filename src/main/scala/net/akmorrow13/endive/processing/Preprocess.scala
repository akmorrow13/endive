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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion

object Preprocess {
  def loadTsv(sc: SparkContext, filePath: String, headerTag: String): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath).filter(r => !r.contains(headerTag))//!r.contains("start") && !r.contains("gene_id") )
    rdd.map( line => {
      line.split("\t")
    })
  }

  def loadLabels(sc: SparkContext, filePath: String): RDD[(ReferenceRegion, Double)] = {
    val rdd = loadTsv(sc, filePath, "start")
    rdd.map(parts => {
      (ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong), extractLabel(parts(3)))
    })
  }

  def loadTranscripts(sc: SparkContext, filePath: String): RDD[Gene] = {
    val rdd = loadTsv(sc, filePath, "##")
              .filter(parts => parts(2) == "transcript")
    val recs = rdd.map(parts => {
      val attrs =
        parts(8).split("; ")
          .map(s => {
            val eqIdx = s.indexOf(" ")
            (s.take(eqIdx), s.drop(eqIdx + 1).replaceAll("\"", ""))
          }).toMap
      (ReferenceRegion(parts(0), parts(3).toLong, parts(4).toLong), attrs.get("gene_id").get, attrs.get("transcript_id").get)
    })
    recs.groupBy(_._2).map(r => Gene(r._1, r._2.map(_._1), r._2.map(_._3)))
  }

  /**
   * Loads narrowPeak files, which are tab delimited peak files
   * see https://genome.ucsc.edu/FAQ/FAQformat.html
   *
   * @param sc
   * @param filePath
   */
  def loadPeaks(sc: SparkContext, filePath: String): RDD[(ReferenceRegion, PeakRecord)] = {
    assert(filePath.endsWith("narrowPeak"))
    val rdd = loadTsv(sc, filePath, "any")
    rdd.map(parts => {
      val region = ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong)
      val l = parts.drop(3).toList.filter(r => r != ".")
      val score = l(0).toInt
      val signalValue = l(1).toDouble
      val pValue = l(2).toDouble
      val qValue = l(3).toDouble
      val peak = l(4).toDouble
      (region, PeakRecord(score, signalValue, pValue, qValue, peak))
    })
  }

  def loadRNA(sc: SparkContext, filePath: String): RDD[RNARecord] = {
    val data = Preprocess.loadTsv(sc, filePath, "gene_id")
    data.map(parts => {
        RNARecord(parts(0), parts(1), parts(2).toDouble, parts(3).toDouble, parts(4).toDouble, parts(5).toDouble, parts(6).toDouble)
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
case class RNARecord(geneId: String, transcriptId: String, length: Double, effective_length: Double,	expected_count: Double,	TPM: Double,	FPKM: Double)

/**
 *
 * @param score Indicates how dark the peak will be displayed in the browser (0-1000). If all scores were '0' when the data were submitted to the DCC, the DCC assigned scores 1-1000 based on signal value. Ideally the average signalValue per base spread is between 100-1000.
strand - +/- to denote strand or orientation (whenever applicable). Use '.' if no orientation is assigned.
 * @param signalValue Measurement of overall (usually, average) enrichment for the region.
 * @param pValue Measurement of statistical significance (-log10). Use -1 if no pValue is assigned.
 * @param qValue Measurement of statistical significance using false discovery rate (-log10). Use -1 if no qValue is assigned.
 * @param peak Point-source called for this peak; 0-based offset from chromStart. Use -1 if no point-source called.
 */
case class PeakRecord(score: Int, signalValue: Double, pValue: Double, qValue: Double, peak: Double)


case class Gene(geneId: String, regions: Iterable[ReferenceRegion], transcripts: Iterable[String])
