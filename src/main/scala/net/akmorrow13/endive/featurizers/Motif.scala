package net.akmorrow13.endive.featurizers

import net.akmorrow13.endive.utils.{ Window, LabeledWindow}
import net.akmorrow13.endive.processing._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import org.bdgenomics.adam.rdd.GenomicRegionPartitioner
import org.bdgenomics.utils.misc.MathUtils
import scala.annotation.tailrec
import scala.beans.BeanProperty
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

}
