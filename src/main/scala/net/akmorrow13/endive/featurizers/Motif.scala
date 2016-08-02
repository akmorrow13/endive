package net.akmorrow13.endive.featurizers

import java.io.File
import org.apache.hadoop.mapred.FileAlreadyExistsException

import net.akmorrow13.endive.processing.Preprocess
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.sys.process._
import scala.util.Random

class Motif(@transient sc: SparkContext, deepbindPath: String) extends Serializable {

  def scoreSequences(sequences: RDD[String], tfs: List[String]): RDD[Map[String, Array[Double]]] = {
    val dbScores: RDD[Map[String, Array[Double]]] = getDeepBindScores(sequences, tfs)
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
                        tfs: List[String]): RDD[Map[String, Array[Double]]] = {

    // local locations of files to read and write from
    val idFile = new File(s"${deepbindPath}/tfDatabase.ids").getPath

    val db = Preprocess.loadCsv(sc, idFile, "Protein")

    // filter db by tfs we want to access
    val filteredDb: RDD[Array[String]] = db.map(r => r(0).split(" # "))
        .map(r => Array(r(0), r(1)))

    // filter out parameters included in db but not in parameters folder
    val tfDatabase: Map[String, String] = filteredDb.collect.map(r => (r(0), r(1))).toMap
    sequences.map(s => getDeepBindScore(s, tfDatabase, idFile))
  }


  /**
   *
   * @param sequence sequence to score with deepbind
   * @param tfDatabase list of tfs to score over
   * @param idFile file of ids (filename.ids) taken in by deepbind
   * @return
   */
  def getDeepBindScore(sequence: String,
                       tfDatabase: Map[String, String],
                      idFile: String): Map[String, Array[Double]] = {

    // run deepbind if data has not yet been saved
    val err: Int = s"${deepbindPath}/deepbindArg ${idFile} ${sequence}" !

    val result: String = s"${deepbindPath}/deepbindArg ${idFile} ${sequence}" !!

    val lines = result.split("\n")(0).split("\t")
    // Array[(tf identifier, score)]
    val scores = lines.zip(result.split("\n")(1).split("\t").map(_.toDouble))

    // Map scores to the name of transcription factor
    scores.map(r => (tfDatabase(r._1), r._2))
    .groupBy(_._1).map(r => (r._1, r._2.map(_._2)))
  }
}