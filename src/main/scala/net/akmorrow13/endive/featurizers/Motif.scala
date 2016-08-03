package net.akmorrow13.endive.featurizers

import java.io.{PrintWriter, File}
import org.apache.hadoop.mapred.FileAlreadyExistsException

import net.akmorrow13.endive.processing.Preprocess
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.io.LocalFileByteAccess
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random

class Motif(@transient sc: SparkContext, deepbindPath: String) extends Serializable {

  def scoreSequences(sequences: RDD[String], tfs: List[String]): RDD[Map[String, Double]] = {
    val dbScores: RDD[Map[String, Double]] = getDeepBindScores(sequences, tfs)
    // TODO: should average over all scoring metrics
    dbScores
  }

  /**
   * gets deepbind scores for tfs
   * WARNING: DEEPBIND DOES NOT HAVE PARAMETERS FOR CREB1
   * @param sequences
   * @param tfs
   * @return
   */
  def getDeepBindScores(sequences: RDD[String],
                        tfs: List[String]): RDD[Map[String, Double]] = {

    // local locations of files to read and write from
    val idFile = new File(s"${deepbindPath}/tfDatabase.ids").getPath

    val bufferedSource = scala.io.Source.fromFile(idFile)
    val buffer: ArrayBuffer[Array[String]] = new ArrayBuffer[Array[String]]()
    for (line <- bufferedSource.getLines) {
      buffer += line.split(",").map(_.trim)
    }
    bufferedSource.close


    val db = buffer.toArray

    // filter db by tfs we want to access
    val filteredDb: Array[Array[String]] = db.map(r => r(0).split(" # "))
      .map(r => Array(r(0), r(1)))

    // filter out parameters included in db but not in parameters folder
    val tfDatabase: Map[String, String] = filteredDb.map(r => (r(0), r(1))).toMap
    sequences.map(s => getDeepBindScore(s, tfDatabase, idFile))
  }

  /**
   * gets deepbind scores for tfs
   * WARNING: DEEPBIND DOES NOT HAVE PARAMETERS FOR CREB1
   * @param sequences
   * @param tfs
   * @return
   */
  def getDeepBindScoresPerPartition(sequences: RDD[String],
                        tfs: List[String]): RDD[Array[Double]] = {

    // local locations of files to read and write from
    val idFile = new File(s"${deepbindPath}/tfDatabase.ids").getPath

    val bufferedSource = scala.io.Source.fromFile(idFile)
    val buffer: ArrayBuffer[Array[String]] = new ArrayBuffer[Array[String]]()
    for (line <- bufferedSource.getLines) {
      buffer += line.split(",").map(_.trim)
    }
    bufferedSource.close


    val db = buffer.toArray

    // filter db by tfs we want to access
    val filteredDb: Array[Array[String]] = db.map(r => r(0).split(" # "))
      .map(r => Array(r(0), r(1)))
    val tfDatabase: Map[String, String] = filteredDb.map(r => (r(0), r(1))).toMap


    sequences.mapPartitions(r => {
      // save sequences to path
      val rand = new scala.util.Random(7)
      val inPath = s"/tmp/${rand.nextString(7)}"
      val sequenceFile = new File(inPath)
      val pw = new PrintWriter(sequenceFile)
      pw.write(r.mkString("\n"))
      pw.close

      // read sequences
      val result: String = s"${deepbindPath}/deepbind ${idFile} > ${inPath}" !!

      val header: Array[String] = result.split("\n")(0).split("\t")
      val lines = result.split("\n").drop(1) // drop header
      val scores: Array[Array[Double]] = lines.map(line => {
        header.zip(line.split("\t").map(_.toDouble))  // zip header with scores for each line
          .map(r => (tfDatabase(r._1), r._2)) // map scores to tf name
          .groupBy(_._1).map(r => (r._1, (r._2.map(_._2).sum/r._2.length))).map(_._2).toArray // calculate averages for tf
        })
      // delete file
      sequenceFile.delete()
      scores.toIterator
    })

  }


  /**
   *
   * @param sequence sequence to score with deepbind
   * @param tfDatabase list of tfs to score over
   * @param idFile file of ids (filename.ids) taken in by deepbind
   * @return map of array of double scores
   */
  def getDeepBindScore(sequence: String,
                       tfDatabase: Map[String, String],
                      idFile: String): Map[String, Double] = {

    val result: String = s"${deepbindPath}/deepbindArg ${idFile} ${sequence}" !!

    val header = result.split("\n")(0).split("\t")
    // Array[(tf identifier, score)]
    val scores = header.zip(result.split("\n")(1).split("\t").map(_.toDouble))

    // Map scores to the name of transcription factor
    scores.map(r => (tfDatabase(r._1), r._2))
    .groupBy(_._1).map(r => (r._1, (r._2.map(_._2).sum/r._2.length)))  // get average score for tf
  }
}