package net.akmorrow13.endive.featurizers

import java.io.{PrintWriter, File}
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.utils.{GenomicRegionPartitioner, Window, LabeledWindow}
import net.akmorrow13.endive.processing._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.bdgenomics.utils.misc.MathUtils
import scala.annotation.tailrec
import scala.beans.BeanProperty
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random
import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConversions._

class ModelConf extends Serializable {
  @BeanProperty var values: java.util.LinkedHashSet[ModelPWM] = null
}

class ModelPWM extends Serializable {
  @BeanProperty var encoding_type: String = null
  @BeanProperty var model_type: String = null
  @BeanProperty var motif_id: String = null

  @BeanProperty var pwm: Array[Array[Double]] = null

  @BeanProperty var tf_id: String = null
  @BeanProperty var tf_name: String = null
  @BeanProperty var tf_species: String = null
}


object Motif {
  def parseYamlMotifs(filePath: String):  List[Motif] = {
    assert(filePath.endsWith("yaml"))

    val configfile = scala.io.Source.fromFile(filePath)
    val configtext = try configfile.mkString finally configfile.close()
    val yaml = new Yaml(new Constructor(classOf[ModelConf]))
    val pwms = yaml.load(configtext).asInstanceOf[ModelConf]
    pwms.values.toList.map(p => Motif(p.getTf_name, p.getPwm.flatten))
  }
}

// Note: this code was copied from net.fnothaft.fig. Move back when fig is not broken
case class Motif(label: String,
                 pwm: Array[Double]) {
  require(pwm.length % 4 == 0, "Position weight matrix length must be a multiple of 4.")
  val length = pwm.length / 4

  // each value must be between 0 and 1
  pwm.foreach(p => require(p >= 0.0 && p <= 1.0, "P = %f must be between [0, 1].".format(p)))

  // check that each row sums to 1.0
  (0 until length).foreach(i => {
    val idx = i * 4
    val p = pwm(idx) + pwm(idx + 1) + pwm(idx + 2) + pwm(idx + 3)
    require(MathUtils.fpEquals(p, 1.0),
      "Probability (p = %f) of row %d was not equal to 1 for length %d motif for %s.".format(p,
        i,
        length,
        label))
  })

  private def baseToIdx(c: Char): Int = c match {
    case 'A' => 0
    case 'C' => 1
    case 'G' => 2
    case 'T' => 3
    case _ => throw new IllegalStateException("Invalid character %s.".format(c))
  }

  def sequenceProbability(sequence: String): Double = {
    @tailrec def score(seq: Iterator[Char],
                       pos: Int,
                       p: Double): Double = {
      // continue until we run out of bases
      // if p = 0, we can terminate early
      if (!seq.hasNext || p == 0.0) {
        p
      } else {
        score(seq, pos + 1, p * pwm(pos * 4 + baseToIdx(seq.next)))
      }
    }

    score(sequence.toIterator, 0, 1.0)
  }
}

class MotifScorer(@transient sc: SparkContext,
            sd: SequenceDictionary) extends Serializable {


  def scoreMotifs(in: RDD[LabeledWindow],
                  windowSize: Int,
                  stride: Int,
                  tfs: Array[TranscriptionFactors.Value],
                  motifDB: String): RDD[LabeledWindow] = {

    // transcription factor specific motifs
    val motifs: RDD[(ReferenceRegion, TranscriptionFactors.Value, PeakRecord)] = Preprocess.loadMotifFolder(sc, motifDB, Some(tfs))
            .map(r => (r._2.region, r._1, r._2))
    val windowedMotifs = CellTypeSpecific.window(motifs, sd)
          .map(r => ((r._1._1, r._1._2), r._2))

    val str = stride
    val win = windowSize

    val x: RDD[LabeledWindow] = in.filter(r => tfs.contains(r.win.getTf))
      .keyBy(r => (r.win.getRegion, r.win.getTf))
      .partitionBy(GenomicRegionPartitioner(Dataset.partitions, sd))
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
                         tfs: List[TranscriptionFactors.Value],
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

    // filter db by tfs we want to access
    val filteredDb: Array[Array[String]] = db.map(r => r(0).split(" # "))
      .map(r => Array(r(0), r(1)))

    // filter out parameters included in db but not in parameters folder
    val tfDatabase: Map[String, String] =
      filteredDb.map(r => (r(0), r(1))).toMap

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
                        tfs: List[TranscriptionFactors.Value],
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
    // Array[(tf identifier (i.e. 'D00032,2'), score)]
    val scores = header.zip(result.split("\n")(1).split("\t").map(_.toDouble))

    // Map scores to the name of transcription factor
    scores.map(r => (tfDatabase(r._1), r._2))
    .groupBy(_._1).map(r => (r._1, (r._2.map(_._2).sum/r._2.length)))  // get average score for tf
  }
}
