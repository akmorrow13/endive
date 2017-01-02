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
    Window(tf, cellType, region, sequence, dnasePeakCount, dnase.getOrElse(DenseVector[Double]()), rnaseq.getOrElse(List()), motifs.getOrElse(List()))
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
    Window(tfValue, cellTypeValue, region, sequence, dnasePeakCount, dnase.getOrElse(DenseVector[Double]()), rnaseq.getOrElse(List()), motifs.getOrElse(List()))
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
case class Window(tf: TranscriptionFactors.Value,
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
  def getRnaseq: List[RNARecord] = rnaseq
  def getMotifs: List[PeakRecord] = motifs

  def setMotifs(motifs: List[PeakRecord]): Window = {
    new Window(this.tf, this.cellType, this.region, this.sequence, this.dnasePeakCount, this.getDnase, this.getRnaseq, motifs)
  }

  def setDnase(dnase: DenseVector[Double]): Window = {
    new Window(this.tf, this.cellType, this.region, this.sequence, this.dnasePeakCount, dnase, this.getRnaseq, this.motifs)
  }

  override
  def toString:String = {
    val stringifiedDnase = dnase.toArray.toList.mkString(",")
    val stringifiedMotifs = motifs.map(_.toString).mkString(Window.EPIDELIM)
    val stringifiedRNAseq = rnaseq.map(_.toString).mkString(Window.EPIDELIM)
    s"${tf.toString},${cellType.toString},${region.referenceName},${region.start},${region.end},${sequence},${dnasePeakCount}${Window.OUTERDELIM}${stringifiedDnase}${Window.OUTERDELIM}${stringifiedRNAseq}${Window.OUTERDELIM}${stringifiedMotifs}"
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
