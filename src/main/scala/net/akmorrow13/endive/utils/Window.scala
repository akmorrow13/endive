package net.akmorrow13.endive.utils

import net.akmorrow13.endive.processing.PeakRecord
import org.bdgenomics.adam.models.ReferenceRegion


/* Base data class */
case class Window(tf: String,
                  cellType: String,
                  region: ReferenceRegion,
                  sequence: String,
                  dnase: List[PeakRecord]) {
  override
  def toString:String = {
    val stringifiedDnase = dnase.map(_.toString).mkString(";")
    s"${tf},${cellType},${region.referenceName},${region.start},${region.end},${sequence};${stringifiedDnase}"
  }
}

case class LabeledWindow(win: Window, label: Int) {
  override
  def toString:String = {
    s"${label},${win.toString}"
  }
}
