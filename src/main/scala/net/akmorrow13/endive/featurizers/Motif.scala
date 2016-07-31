package net.akmorrow13.endive.featurizers

import java.io.File
import org.apache.hadoop.mapred.FileAlreadyExistsException

import net.akmorrow13.endive.processing.Preprocess
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.sys.process._

class Motif(@transient sc: SparkContext, deepbindPath: String) {

  def scoreSequences(tfs: List[String], sequences: RDD[String]): RDD[Map[String, Double]] = {
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

    // locations of files to read and write from
    val idLocation = deepbindPath + "/testdb.ids"
    val seqLocation = deepbindPath + "/testsequences.seq"
    val scoreLocation = deepbindPath + "/testscores.seq"

    val dbPath = deepbindPath + "/db/db.tsv"
    val db = Preprocess.loadTsv(sc, dbPath, "Protein")

    // filter db by tfs
    var filteredDb: RDD[Array[String]] = db.filter(r => tfs.contains(r(1)))

    // filter out tfs not found in database
    val paramPath = deepbindPath + "/db/params/"
    val available: String = Seq("ls", s"${paramPath}" ).!!
    val availableParams: Array[String] = available.split("\n").map(r => {
      r.dropRight(4)
    })

    // filter out parameters included in db but not in parameters folder
    filteredDb = filteredDb.filter(r => availableParams.contains(r(0)))

    // create id page to score sequences of the form:
    //  - DEEPBIND_ID # NAME ORIGIN
    try {
      val ids = filteredDb.map(r => s"${r(0)} # ${r(1)}, ${r(6)}").repartition(1)
      // save labels to be accessed by deepbind
      ids.saveAsTextFile(idLocation)
      val cmd = Seq("mv", s"${idLocation}/part-00000" , s"${idLocation}/part-00000.ids" ).!
      println("saved ids ", ids.count)
    } catch {
      case e: FileAlreadyExistsException => {
      println("ids file already found. skipping...")
      }
    }

    try {
      // save sequences
      sequences
        .repartition(1)
        .saveAsTextFile(seqLocation)
      // rename files to have .seq
      val cmd = Seq("mv", s"${seqLocation}/part-00000" , s"${seqLocation}/part-00000.seq" ).!
      println("saved sequences ", sequences.count)

    } catch {
      case e: FileAlreadyExistsException => {
        println("seq file already found. skipping...")
      }
    }

    // run deepbind if data has not yet been saved
    if (!new File(scoreLocation).exists()) {
      val scores = s"${deepbindPath}/deepbind ${idLocation}/part-00000.ids ${seqLocation}/part-00000.seq" #>> new File(scoreLocation) !
    }
    // return deepbind scores
    val labels: Array[String] = filteredDb.map(r => r(1)).collect

    val finalScores = sc.textFile(scoreLocation).filter(l => !l.contains("D")) // filter out first line
                .map( line => {
                   labels.zip(line.split("\t"))
                                .map(r => (r._1, r._2.toDouble))
                                .groupBy(_._1)
                                .map(r => (r._1,r._2.map(_._2).sum)).toMap
                })

    finalScores
  }
}