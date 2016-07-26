package net.akmorrow13.endive.featurizers

import java.io.File

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


  def getDeepBindScores(sequences: RDD[String],
                        tfs: List[String]): RDD[Map[String, Double]] = {

    val idLocation = deepbindPath + "/testdb.ids"
    val seqLocation = deepbindPath + "/testsequences.seq"
    val scoreLocation = deepbindPath + "/testscores.seq"

    val dbPath = deepbindPath + "/db/db.tsv"
    val db = Preprocess.loadTsv(sc, dbPath, "ID")
    // filter db by tfs
    val filteredDb: RDD[Array[String]] = db.filter(r => tfs.contains(r(1)))

    // create id page to score sequences of the form:
    //  - DEEPBIND_ID # NAME ORIGIN
    val ids = filteredDb.map(r => s"${r(0)},# ${r(1)}, ${r(6)}")
    println("saving ids", ids.count)

    // save labels to be accessed by deepbind
    ids.saveAsTextFile(idLocation)

    // save sequences
    sequences.saveAsTextFile(seqLocation)

    // run deepbind

    val cmd: String = Seq("./${deepbindPath}/deepbind ", idLocation, " < ", seqLocation, " > ", scoreLocation).!!
    println(cmd)

    // return deepbind scores
    val labels: Array[String] = filteredDb.map(r => r(1)).collect

    val finalScores = sc.textFile(scoreLocation).filter(l => !l.contains("D")) // filter out first line
                  .map( line => {
                     labels.zip(line.split("\t"))
                                  .map(r => (r._1, r._2.toDouble))
                                  .groupBy(_._1)
                                  .map(r => (r._1,r._2.map(_._2).sum)).toMap
                  })


    // remove saved files
    var rm : String = s"rm -r ${idLocation}"
    Runtime.getRuntime().exec(rm)
    rm = s"rm -r ${seqLocation}"
    Runtime.getRuntime().exec(rm)

    finalScores
  }
}