package net.akmorrow13.endive.utils

import java.io.ByteArrayOutputStream
import net.akmorrow13.endive.processing.{RNARecord, PeakRecord}
import org.bdgenomics.adam.models.ReferenceRegion
import scala.util.{ Try, Success, Failure }

/**
 * required to standardize cell type names
 */
object Window {
  def apply(tf: String,
             cellType: String,
             region: ReferenceRegion,
             sequence: String,
             dnase: Option[List[PeakRecord]] = None,
             rnaseq: Option[List[RNARecord]] = None): Window = {
    Window(tf, filterCellTypeName(cellType), region, sequence, dnase.getOrElse(List()), rnaseq.getOrElse(List()))
  }

  def filterCellTypeName(cellType: String): String = {
    cellType.filterNot("-_".toSet)
  }
  /* TODO this is a hack
   * We should turn everything into Avro objects to serialize */

  /* Delimiter between Sequence DNASE and RNASE  */
  val OUTERDELIM = "\\|\\|"

  /* Delimiter inside Sequence and label*/
  val CHIPSEQDELIM = ","

  /* Delimiter to split RNASE AND DNASE windows */
  val EPIDELIM= ";"

}

/* Base data class */
case class Window(tf: String,
                  cellType: String,
                  region: ReferenceRegion,
                  sequence: String,
                  dnase: List[PeakRecord],
                  rnaseq: List[RNARecord]) extends Serializable {

  def getRegion: ReferenceRegion = region
  def getTf: String = tf
  def getCellType: String = cellType
  def getSequence: String = sequence
  def getDnase: List[PeakRecord] = dnase
  def getRnaseq: List[RNARecord] = rnaseq

  override
  def toString:String = {
    val stringifiedDnase = dnase.map(_.toString).mkString(Window.DNARNADELIM)
    val stringifiedRNAseq = rnaseq.map(_.toString).mkString(Window.DNARNADELIM)
    s"${tf},${cellType},${region.referenceName},${region.start},${region.end},${sequence}${Window.OUTERDELIM}${stringifiedDnase}${Window.OUTERDELIM}${stringifiedRNAseq}"
  }
}

case class LabeledWindow(win: Window, label: Int) {
  override
  def toString:String = {
    s"${label},${win.toString}"
  }
}
