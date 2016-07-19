package net.akmorrow13.endive.utils


/* Base data class */
case class Window(startIdx: Long,
                  endIdx: Long,
                  sequence: String) {
  override
  def toString:String = {
    s"${startIdx},${endIdx},${sequence}"
  }
}



case class LabeledWindow(win: Window, label: Long) {

  override
  def toString:String = {
    s"${win.toString}, ${label}"
  }
}
