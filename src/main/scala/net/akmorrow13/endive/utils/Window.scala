package net.akmorrow13.endive.utils

import net.akmorrow13.endive.processing.{RNARecord, PeakRecord}
import org.bdgenomics.adam.models.ReferenceRegion

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
    val filteredCellType: String = cellType.filterNot("-_".toSet)
    new Window(tf, filteredCellType, region, sequence, dnase.getOrElse(List()), rnaseq.getOrElse(List()))
  }
}

/* Base data class */
class Window(tf: String,
                  cellType: String,
                  region: ReferenceRegion,
                  sequence: String,
                  dnase: List[PeakRecord],
                  rnaseq: List[RNARecord]) {

  def getRegion: ReferenceRegion = region
  def getTf: String = tf
  def getCellType: String = cellType
  def getSequence: String = sequence
  def getDnase: List[PeakRecord] = dnase
  def getRnaseq: List[RNARecord] = rnaseq

  override
  def toString:String = {
    val stringifiedDnase = dnase.map(_.toString).mkString(";")
    val stringifiedRNAseq = rnaseq.map(_.toString).mkString(";")
    s"${tf},${cellType},${region.referenceName},${region.start},${region.end},${sequence}-${stringifiedDnase}-${stringifiedRNAseq}"
  }
}

case class LabeledWindow(win: Window, label: Int) {
  override
  def toString:String = {
    s"${label},${win.toString}"
  }
}
