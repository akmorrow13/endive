package net.akmorrow13.endive.utils


/* Base data class */
case class Window(chrosomeName: String,
                  startIdx: Long,
                  endIdx: Long,
                  sequence: String) {
  override
  def toString:String = {
    s"${chrosomeName},${startIdx},${endIdx},${sequence}"
  }
}



case class LabeledWindow(win: Window, label: Double) {

  override
  def toString:String = {
    s"${win.toString}, ${label}"
  }
}
