package net.akmorrow13.endive.utils

import java.io.ByteArrayOutputStream
import net.akmorrow13.endive.processing._
import org.bdgenomics.adam.models.ReferenceRegion
import scala.util.{ Try, Success, Failure }

/**
 * required to standardize cell type names
 */
object Window {
  def apply(tf: TranscriptionFactors.Value,
             cellType: CellTypes.Value,
             region: ReferenceRegion,
             sequence: String,
             dnase: Option[List[PeakRecord]] = None,
             rnaseq: Option[List[RNARecord]] = None,
             motifs: Option[List[PeakRecord]] = None): Window = {
    Window(tf, cellType, region, sequence, dnase.getOrElse(List()), rnaseq.getOrElse(List()), motifs.getOrElse(List()))
  }

  def fromStringNames(tf: String,
            cellType: String,
            region: ReferenceRegion,
            sequence: String,
            dnase: Option[List[PeakRecord]] = None,
            rnaseq: Option[List[RNARecord]] = None,
            motifs: Option[List[PeakRecord]] = None)(implicit s: DummyImplicit): Window = {
    val tfValue = TranscriptionFactors.withName(tf)
    val cellTypeValue = CellTypes.withName(Dataset.filterCellTypeName(cellType))
    Window(tfValue, cellTypeValue, region, sequence, dnase.getOrElse(List()), rnaseq.getOrElse(List()), motifs.getOrElse(List()))
  }

  /* TODO this is a hack
   * We should turn everything into Avro objects to serialize */

  /* Delimiter between Sequence DNASE and RNASE  */
  val OUTERDELIM = "!"

  /* Delimiter inside Sequence and label*/
  val CHIPSEQDELIM = ","

  /* Delimiter to split RNASE AND DNASE windows */
  val EPIDELIM= ";"

}

/* Base data class */
case class Window(tf: TranscriptionFactors.Value,
                  cellType: CellTypes.Value,
                  region: ReferenceRegion,
                  sequence: String,
                  dnase: List[PeakRecord],
                  rnaseq: List[RNARecord],
                  motifs: List[PeakRecord]) extends Serializable {

  def getRegion: ReferenceRegion = region
  def getTf: TranscriptionFactors.Value = tf
  def getCellType: CellTypes.Value = cellType
  def getSequence: String = sequence
  def getDnase: List[PeakRecord] = dnase
  def getRnaseq: List[RNARecord] = rnaseq
  def getMotifs: List[PeakRecord] = motifs

  def setMotifs(motifs: List[PeakRecord]): Window = {
    new Window(this.tf, this.cellType, this.region, this.sequence, this.getDnase, this.getRnaseq, motifs)
  }

  override
  def toString:String = {
    val stringifiedDnase = dnase.map(_.toString).mkString(Window.EPIDELIM)
    val stringifiedMotifs = motifs.map(_.toString).mkString(Window.EPIDELIM)
    val stringifiedRNAseq = rnaseq.map(_.toString).mkString(Window.EPIDELIM)
    s"${tf.id},${cellType.id},${region.referenceName},${region.start},${region.end},${sequence}${Window.OUTERDELIM}${stringifiedDnase}${Window.OUTERDELIM}${stringifiedRNAseq}${Window.OUTERDELIM}${stringifiedMotifs}"
  }
}

case class LabeledWindow(win: Window, label: Int) extends Serializable {
  override
  def toString:String = {
    s"${label},${win.toString}"
  }
}
