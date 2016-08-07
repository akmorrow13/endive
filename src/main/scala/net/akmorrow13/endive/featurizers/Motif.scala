package net.akmorrow13.endive.featurizers

import java.io.{PrintWriter, File}
import net.akmorrow13.endive.utils.{Window, LabeledReferenceRegionPartitioner, LabeledWindow}
import org.apache.hadoop.mapred.FileAlreadyExistsException

import net.akmorrow13.endive.processing.{Dataset, CellTypeSpecific, PeakRecord, Preprocess}
import net.akmorrow13.endive.processing.{CellTypeSpecific, PeakRecord, Preprocess}
>>>>>>> 82f5a34... updated BaseModel for db
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import org.bdgenomics.utils.io.LocalFileByteAccess
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random

class Motif(@transient sc: SparkContext,
            sd: SequenceDictionary) extends Serializable {


  def scoreMotifs(in: RDD[LabeledWindow],
                  windowSize: Int,
                  stride: Int,
                  tfs: Array[String],
                  motifDB: String): RDD[LabeledWindow] = {
    val motifs: RDD[(ReferenceRegion, String, PeakRecord)] = Preprocess.loadMotifFolder(sc, motifDB, Some(tfs))
            .map(r => (r._2.region, r._1, r._2))
    val windowedMotifs = CellTypeSpecific.window[PeakRecord](motifs, sd)

    val str = stride
    val win = windowSize

    val x: RDD[LabeledWindow] = in.filter(r => tfs.contains(r.win.getTf))
      .keyBy(r => (r.win.getRegion, r.win.getCellType))
      .partitionBy(new LabeledReferenceRegionPartitioner(sd, Dataset.cellTypes.toVector))
      .leftOuterJoin(windowedMotifs)
      .map(r => {
        val motifs = r._2._2
        LabeledWindow(Window(r._2._1.win.getTf, r._2._1.win.getCellType,
          r._2._1.win.getRegion, r._2._1.win.getSequence, Some(r._2._1.win.getDnase), Some(r._2._1.win.getRnaseq), motifs), r._2._1.label)
      })
    x
  }


  /**
   * gets deepbind scores for tfs
   * WARNING: DEEPBIND DOES NOT HAVE PARAMETERS FOR CREB1
   * @param sequences
   * @param tfs
   * @return
   */
  def getDeepBindScores(sequences: RDD[String],
                         tfs: List[String],
                         deepbindPath: String): RDD[Map[String, Double]] = {

    // local locations of files to read and write from
    val idFile = new File(s"${deepbindPath}/tfDatabase.ids").getPath
    val bufferedSource = scala.io.Source.fromFile(idFile)
    val buffer: ArrayBuffer[Array[String]] = new ArrayBuffer[Array[String]]()
    for (line <- bufferedSource.getLines) {
      buffer += line.split(",").map(_.trim)
    }
    bufferedSource.close
    val db = buffer.toArray
    println(db)
    // filter db by tfs we want to access
    val filteredDb: Array[Array[String]] = db.map(r => r(0).split(" # "))
      .map(r => Array(r(0), r(1)))

    // filter out parameters included in db but not in parameters folder
    val tfDatabase: Map[String, String] = filteredDb.map(r => (r(0), r(1))).toMap
    sequences.map(s => getDeepBindScore(s, tfDatabase, idFile, deepbindPath))
  }

  /**
   * gets deepbind scores for tfs
   * WARNING: DEEPBIND DOES NOT HAVE PARAMETERS FOR CREB1
   * @param sequences
   * @param tfs
   * @return
   */
  def getDeepBindScoresPerPartition(sequences: RDD[String],
                        tfs: List[String],
                        deepbindPath: String): RDD[Array[Double]] = {

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
                      idFile: String,
                      deepbindPath: String): Map[String, Double] = {
    val result: String = s"${deepbindPath}/deepbindArg ${idFile} ${sequence}" !!

    val header = result.split("\n")(0).split("\t")
    // Array[(tf identifier, score)]
    val scores = header.zip(result.split("\n")(1).split("\t").map(_.toDouble))

    // Map scores to the name of transcription factor
    scores.map(r => (tfDatabase(r._1), r._2))
    .groupBy(_._1).map(r => (r._1, (r._2.map(_._2).sum/r._2.length)))  // get average score for tf
  }
}
