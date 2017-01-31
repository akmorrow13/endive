package net.akmorrow13.endive.utils

import net.akmorrow13.endive.processing._
import org.bdgenomics.adam.models.ReferenceRegion
import breeze.linalg._
import org.bdgenomics.formats.avro.AlignmentRecord

/**
 * required to standardize cell type names
 */
object Window {
  def apply(tf: TranscriptionFactors.Value,
             cellType: CellTypes.Value,
             region: ReferenceRegion,
             sequence: String,
             dnasePeakCount: Int = 0,
             dnase: Option[DenseVector[Double]] = None,
             rnaseq: Option[List[RNARecord]] = None,
             motifs: Option[List[PeakRecord]] = None): Window = {
    new Window(tf, cellType, region, sequence, dnasePeakCount, dnase.getOrElse(DenseVector(Array[Double]())), List(), List())
  }

  def fromStringNames(tf: String,
            cellType: String,
            region: ReferenceRegion,
            sequence: String,
            dnasePeakCount: Int = 0,
            dnase: Option[DenseVector[Double]] = None,
            rnaseq: Option[List[RNARecord]] = None,
            motifs: Option[List[PeakRecord]] = None)(implicit s: DummyImplicit): Window = {
    val tfValue = TranscriptionFactors.withName(tf)
    val cellTypeValue = CellTypes.withName(Dataset.filterCellTypeName(cellType))
    new Window(tfValue, cellTypeValue, region, sequence, dnasePeakCount, dnase.getOrElse(DenseVector(Array[Double]())), List(), List())
  }

  /* TODO this is a hack
   * We should turn everything into Avro objects to serialize */

  /* Delimiter between Sequence DNASE and RNASE  */
  val OUTERDELIM = "!"

  /* Delimiter inside Sequence and label*/
  val STDDELIM = ","

  /* Delimiter inside Features and LabeledWindow */
  val FEATDELIM = "#"

  /* Delimiter to split RNASE AND DNASE windows */
  val EPIDELIM= ";"

}

/* Base data class */
class Window(tf: TranscriptionFactors.Value,
                  cellType: CellTypes.Value,
                  region: ReferenceRegion,
                  sequence: String,
                  dnasePeakCount: Int,
                  dnase: DenseVector[Double],
                  rnaseq: List[RNARecord],
                  motifs: List[PeakRecord]) extends Serializable {

  def getRegion: ReferenceRegion = region
  def getTf: TranscriptionFactors.Value = tf
  def getCellType: CellTypes.Value = cellType
  def getSequence: String = sequence
  def getDnase: DenseVector[Double] = dnase
  def getDnasePeakCount: Int = dnasePeakCount
  def getRnaseq: List[RNARecord] = rnaseq
  def getMotifs: List[PeakRecord] = motifs

  def setMotifs(motifs: List[PeakRecord]): Window = {
    new Window(this.tf, this.cellType, this.region, this.sequence, this.dnasePeakCount, this.getDnase, this.getRnaseq, motifs)
  }

  def setDnase(dnase: DenseVector[Double]): Window = {
    new Window(this.tf, this.cellType, this.region, this.sequence, this.dnasePeakCount, dnase, this.getRnaseq, this.motifs)
  }

  def setDnaseCount(dnaseCount: Int): Window = {
    new Window(this.tf, this.cellType, this.region, this.sequence, dnaseCount, this.getDnase, this.getRnaseq, this.motifs)
  }

  def setRegion(region: ReferenceRegion): Window = {
    new Window(this.tf, this.cellType, region, this.sequence, this.dnasePeakCount, this.getDnase, this.getRnaseq, this.motifs)
  }

  override
  def toString:String = {
    val stringifiedDnase = dnase.toArray.toList.mkString(",")
    val stringifiedMotifs = motifs.map(_.toString).mkString(Window.EPIDELIM)
    val stringifiedRNAseq = rnaseq.map(_.toString).mkString(Window.EPIDELIM)
    s"${tf.toString},${cellType.toString},${region.referenceName},${region.start},${region.end},${sequence},${dnasePeakCount}${Window.OUTERDELIM}${stringifiedDnase}${Window.OUTERDELIM}${stringifiedRNAseq}${Window.OUTERDELIM}${stringifiedMotifs}"
  }

  def merge(other: Window): Window = {
    require(tf.equals(other.getTf), s"tfs do not match: ${tf} vs ${other.getTf}")
    require(cellType.equals(other.getCellType), s"celltypes do not match: ${cellType} vs ${other.getCellType}")
    require(region.overlaps(other.getRegion), s"window regions do not overlap: ${region} vs ${other.getRegion}")

    // order windows by position
    val (window_1, window_2)  =
      if (region.start < other.getRegion.start) (this, other)
      else (other, this)

    // merge sequence
    val newSequence = window_1.getSequence.concat(window_2.getSequence.substring((window_1.getRegion.end - window_2.getRegion.start).toInt))

    // merge dnase
    val newDnase = DenseVector.vertcat(window_1.getDnase,
      window_2.getDnase.slice((window_1.getRegion.end - window_2.getRegion.start).toInt, window_2.getDnase.length))

    // Note: ignoring RNAseq and motif data

    val newRegion = region.merge(other.getRegion)

    val dnasePeakCount = window_1.getDnasePeakCount + window_2.getDnasePeakCount

    Window(tf, cellType, newRegion, newSequence, dnasePeakCount, Some(newDnase))
  }

  /**
   * Slices window at start and end indices relative to region
   * @param start
   * @param end
   * @return
   */
  def slice(start: Long, end: Long): Window = {
    require(start >= region.start && end <= region.end, s"can only slice regions within current region")
    require(start <= end, s"sliced region must have start <= end")

    val baseStart = (start - region.start).toInt
    val baseEnd = (end - region.start).toInt

    val newRegion = ReferenceRegion(region.referenceName, start, end)
    val newSequence = sequence.substring(baseStart, baseEnd)
    val newDnase = this.dnase.slice(baseStart, baseEnd)
    Window(tf, cellType, newRegion, newSequence, dnasePeakCount, Some(newDnase))
  }
}

case class FeaturizedLabeledWindowWithScore(featured: FeaturizedLabeledWindow, score: Double) extends Ordered[FeaturizedLabeledWindowWithScore] {
  def compare(that: FeaturizedLabeledWindowWithScore): Int = this.score.compareTo(that.score)
}


case class FeaturizedLabeledWindow(labeledWindow: LabeledWindow, features: DenseVector[Double]) {

  override def toString: String = {
    labeledWindow.toString + Window.FEATDELIM + features.toArray.mkString(",")
  }
}

case class LabeledWindow(win: Window, label: Int) extends Serializable {
  override
  def toString:String = {
    s"${label},${win.toString}"
  }
}
