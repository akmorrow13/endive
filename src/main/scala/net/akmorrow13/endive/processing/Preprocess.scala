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
import breeze.linalg.DenseVector
import net.akmorrow13.endive.utils.{LabeledWindowLoader, LabeledWindow}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.feature.CoverageRDD
import org.bdgenomics.adam.rdd.read.{AlignedReadRDD, AlignmentRecordRDD}


object Preprocess {

  /**
   * Loads tsv file
    *
    * @param sc SparkContext
   * @param filePath tsv filepath to load
   * @param headerTag tags in first row, if any, that should be excluded from load
   * @return RDD of rows from tsv file
   */
  def loadTsv(sc: SparkContext, filePath: String, headerTag: String, numPartitions: Int = 500): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath, numPartitions).filter(r => !r.contains(headerTag))
    rdd.map( line => {
      line.split("\t")
    })
  }

  /**
   * Loads tsv file
    *
    * @param sc SparkContext
   * @param filePath tsv filepath to load
   * @param headerTag tags in first row, if any, that should be excluded from load
   * @return RDD of rows from tsv file
   */
  def loadCsv(sc: SparkContext, filePath: String, headerTag: String): RDD[Array[String]] = {
    val rdd = sc.textFile(filePath).filter(r => !r.contains(headerTag))
    println(s"Loaded file ${filePath} with ${rdd.count} records")
    rdd.map( line => {
      line.split(",")
    })
  }

  /**
   * Loads labels from all a chipseq label file
   * flatmaps all cell types into an individual datapoint
    *
    * @param sc
   * @param filePath tsv file of chipseq labels
   * @return parsed files of (tf, cell type, region, score)
   */
  def loadLabels(sc: SparkContext, filePath: String, numPartitions: Int = 500): Tuple2[RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)], Array[CellTypes.Value]] = {
    assert(filePath.endsWith("tsv") || filePath.endsWith("tsv.gz"))
    val headerTag = "start"
    // parse header for cell types
    val tsvRDD = sc.textFile(filePath, numPartitions)
    val cellTypes = tsvRDD.filter(r => r.contains(headerTag)).first().split("\t").drop(3).map(r => CellTypes.getEnumeration(r))
    val file = filePath.split("/").last
    // parse file name for tf
    val tf = TranscriptionFactors.withName(file.split('.')(0))
    println(s"loading  labels for cell type ${cellTypes.mkString} from file ${file}")
    val tsvRDDSplit = tsvRDD.filter(r => !r.contains(headerTag)).map(_.split("\t"))

    val result = tsvRDDSplit.flatMap(parts => {
      cellTypes.zipWithIndex.map( cellType => {
        (tf, cellType._1, ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong), extractLabel(parts(3 + cellType._2)))
      })
    })
    (result, cellTypes)
  }

  def loadLabelFolder(sc: SparkContext, folder: String): RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)] = {
    var data: RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)] = sc.emptyRDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)]
    val d = new File(folder)
    if (sc.isLocal) {
      if (d.exists && d.isDirectory) {
        val files = d.listFiles.filter(_.isFile).toList
        files.map(f => {
          data = data.union(loadLabels(sc, f.getPath)._1)
        })
      } else {
        throw new Exception(s"${folder} is not a valid directory for peaks")
      }
    } else {
    try{
      val fileNames = getFileNamesFromDirectory(sc, folder)
      for (file <- fileNames) {
        data = data.union(loadLabels(sc, file)._1)
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
    *
    * @param sc
   * @param filePath
   * @return
   */
  def loadTranscripts(sc: SparkContext, filePath: String): RDD[Transcript] = {
    // extract cell type

    val rdd = loadTsv(sc, filePath, "##")
    val transcripts = mapAttributes(
      rdd
      .filter(parts => parts(2) == "gene"))

    transcripts.map(r => Transcript(r._2, r._3, r._1))

  }

  /**
   * Maps attributes in gtf file for gtf files delimited with ';'
    *
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
   * Loads narrowPeak files, which are tab delimited peak files. These store cell type specific information
   * see https://genome.ucsc.edu/FAQ/FAQformat.html
   *
   * @param sc
   * @param filePath
   */
  def loadPeaks(sc: SparkContext, filePath: String): RDD[(CellTypes.Value, PeakRecord)] = {
    val cellType = CellTypes.getEnumeration(filePath.split("/").last.split('.')(1))
    val rdd = loadTsv(sc, filePath, "any")
    rdd.map(parts => {
      val region = ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong)
      val l = parts.drop(3)
      val score = l(1).toInt
      val signalValue = l(3).toDouble
      val pValue = l(4).toDouble
      val qValue = l(5).toDouble
      val peak = l(6).toDouble
      (cellType, PeakRecord(region, score, signalValue, pValue, qValue, peak))
    })
  }


  /**
   * Loads narrowPeak files, which are tab delimited peak files
   * see https://genome.ucsc.edu/FAQ/FAQformat.html
   *
   * @param sc
   * @param filePath
   */
  def loadWigs(sc: SparkContext, filePath: String): RDD[(CellTypes.Value, PeakRecord)] = {
    val cellType = CellTypes.getEnumeration(filePath.split("/").last.split('.')(1))
    val rdd = loadTsv(sc, filePath, "#")
    rdd.map(parts => {
      val region = ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong)
      val signalValue = parts(3).toDouble
      (cellType, PeakRecord(region, 0, signalValue, 0, 0, 0))
    })
  }

  /**
   * Loads motif files, which are tab delimited peak files
   * see https://genome.ucsc.edu/FAQ/FAQformat.html
   *
   * @param sc
   * @param filePath
   * @return rdd of motifs mapped by (transcription factor, peakrecord with pvalue and peak specified)
   */
  def loadMotifs(sc: SparkContext, filePath: String): RDD[(TranscriptionFactors.Value, PeakRecord)] = {
    val tf = TranscriptionFactors.withName(filePath.split("/").last.split('_')(0))
    println(s"loading motifs for ${tf.toString}")
    val rdd = loadTsv(sc, filePath, "#pattern")
    rdd.map(parts => {
      val region = ReferenceRegion(parts(1), parts(2).toLong, parts(3).toLong)
      val l = parts.drop(3).toList.filter(r => r != ".")
      val peak = parts(5).toDouble
      val pValue = parts(6).toDouble
      (tf, PeakRecord(region, -1, -1, pValue, -1, peak))
    })
  }

  def loadMotifFolder(sc: SparkContext, folder: String, tfs: Option[Array[TranscriptionFactors.Value]]): RDD[(TranscriptionFactors.Value, PeakRecord)] = {

    var data: RDD[(TranscriptionFactors.Value, PeakRecord)] = sc.emptyRDD[(TranscriptionFactors.Value, PeakRecord)]
    if (sc.isLocal) {
      val d = new File(folder)
      if (d.exists && d.isDirectory) {
        val files = d.listFiles.filter(_.isFile).toList
        files.map(f => {
          data = data.union(loadMotifs(sc, f.getPath))
        })
      } else {
        throw new Exception(s"${folder} is not a valid directory for peaks")
      }
    } else {
      try{
        val fs: FileSystem = FileSystem.get(new Configuration())
        val status = { // filter by transcription factors
          val s = fs.listStatus(new Path(folder))
          if (tfs.isDefined) {
            s.filter(r => tfs.get.contains(r.getPath.getName.split("_")(0)))
          } else s
        }
        for (i <- status) {
          val file: String = i.getPath.toString
          data = data.union(loadMotifs(sc, file))
        }
      } catch {
        case e: Exception => println(s"Directory ${folder} could not be loaded")
      }
    }
    data
  }

  /**
   * gets a list of file names in a directory
    *
    * @param sc
   * @param directory
   * @return Array of filenames
   */
  def getFileNamesFromDirectory(sc: SparkContext, directory: String): Array[String] = {
    try{
      val fs: FileSystem = FileSystem.get(new Configuration())
      val fileNames: Array[String] = fs.listStatus(new Path(directory)).map(_.getPath.toString)
      fileNames
    } catch {
      case e: Exception => println(s"Directory ${directory} could not be loaded")
        null
    }
  }


  def loadPeakFolder(sc: SparkContext, folder: String): RDD[(CellTypes.Value, PeakRecord)] = {

    var data: RDD[(CellTypes.Value, PeakRecord)] = sc.emptyRDD[(CellTypes.Value, PeakRecord)]
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
        val fileNames = getFileNamesFromDirectory(sc, folder)
        for (file <- fileNames) {
        data = data.union(loadPeaks(sc, file))
      }
      } catch {
        case e: Exception => println(s"Directory ${folder} could not be loaded")
      }
    }
    data
  }

  def loadPeakFiles(sc: SparkContext, files: Array[String]): RDD[(CellTypes.Value, PeakRecord)] = {
    var data: RDD[(CellTypes.Value, PeakRecord)] = sc.emptyRDD[(CellTypes.Value, PeakRecord)]
    for (f <- files) {
      val temp = loadPeaks(sc, f)
      data = data.union(temp)
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

  /**
   * Parses folder of dnase files that load into AlignmentRecordRDDs. File
   * naming convention for individual adam files is alignmentcuts.DNASE.<CELLTYPE_NAME>.adam
   * @param sc Spark Context
   * @param dnasePath path containing individual adam files for each cell Type
   * @param cellType cell type to be loaded
   */
  def loadDnase(sc: SparkContext,
                dnasePath: String,
                cellType: CellTypes.Value): (CoverageRDD, CoverageRDD) = {

    val fs: FileSystem = FileSystem.get(new Configuration())

    // read all bams in file and save positive coverage (positive and negative files for each)
    val status: Array[FileStatus] = fs.listStatus(new Path(dnasePath))
      .filter(i => i.getPath.getName.endsWith(".adam") // verify ADAM file
        && cellType.equals(CellTypes.getEnumeration(i.getPath.getName.split('.')(0)))) // extract relevent celltype

    val positiveStatus = status.filter(r => r.getPath.getName.contains("positive"))
    val negativeStatus = status.filter(r => r.getPath.getName.contains("negative"))

    println(s"loaded positives and negatives")
    positiveStatus.foreach(r => println(r.getPath.getName))
    negativeStatus.foreach(r => println(r.getPath.getName))

    val (positiveCoverage) =
      positiveStatus.map(i => {
        val filePath: String = i.getPath.toString
        val fileName = i.getPath.getName
        val cell = fileName.split('.')(2)
        println(s"processing file ${fileName} for cellType ${cell} from ${dnasePath}")

        // get positive strand coverage
        val alignments = sc.loadCoverage(filePath)
        (alignments.rdd, alignments.sequences)
      }).reduce((r1, r2) =>  (r1._1.union(r2._1), r1._2 ++ r2._2))

    val (negativeCoverage) =
      negativeStatus.map(i => {
        val filePath: String = i.getPath.toString
        val fileName = i.getPath.getName
        val cell = fileName.split('.')(2)
        println(s"processing file ${fileName} for cellType ${cell} from ${dnasePath}")

        // get positive strand coverage
        val alignments = sc.loadCoverage(filePath)
        (alignments.rdd, alignments.sequences)
      }).reduce((r1, r2) =>  (r1._1.union(r2._1), r1._2 ++ r2._2))

    (CoverageRDD(positiveCoverage._1, positiveCoverage._2),
      CoverageRDD(negativeCoverage._1, negativeCoverage._2))
  }

}

/**
 *
 * @param geneId
 * @param length
 * @param effective_length
 * @param expected_count
 * @param TPM: transcripts per million
 * @param FPKM: fragments per kilobase of exon per million reads mapped
*/
case class RNARecord(region: ReferenceRegion, geneId: String, length: Double, effective_length: Double,	expected_count: Double,	TPM: Double,	FPKM: Double) {
  override def toString: String = {
    s"${region.referenceName},${region.start},${region.end},${geneId},${length},${effective_length},${expected_count},${TPM},${FPKM}"
  }
}

object RNARecord {
  def fromString(str: String): RNARecord = {
    val parts = str.split(",")
    val region = ReferenceRegion(parts(0), parts(1).toLong, parts(2).toLong)
    RNARecord(region, parts(3), parts(4).toDouble, parts(5).toDouble, parts(6).toDouble, parts(7).toDouble, parts(8).toDouble)

  }
}

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

case class BaseFeature(labeledWindow: LabeledWindow, features: DenseVector[Double]) {

  override def toString: String = {
    labeledWindow.toString + features.toString
  }
}

object BaseFeatureLoader {

  def stringToBaseFeature(str: String): BaseFeature = {
    val parts = str.split("DenseVector")
    val winStr = parts(0)
    val featureStr = parts(1).dropRight(1).drop(1)
    val vector = DenseVector[Double](featureStr.split(",").map(_.toDouble))
    BaseFeature(LabeledWindowLoader.stringToLabeledWindow(winStr), vector)
  }

  def apply(path: String, sc: SparkContext): RDD[BaseFeature] = {
    val dataTxtRDD:RDD[String] = sc.textFile(path)
    dataTxtRDD.map(stringToBaseFeature(_))
  }
}

