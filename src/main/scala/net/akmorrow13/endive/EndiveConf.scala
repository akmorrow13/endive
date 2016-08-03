package net.akmorrow13.endive
import scala.reflect.{BeanProperty, ClassTag}

class EndiveConf extends Serializable {
  @BeanProperty var createWindows: Boolean = false
  /* These are required if createWindows is False */

  @BeanProperty var windowLoc: String = null
  @BeanProperty var folds: Int = 2
  @BeanProperty var sequenceLoc: String = null
  @BeanProperty var rnaseqLoc: String = null
  @BeanProperty var dnaseLoc: String = null

  /* output files */
  @BeanProperty var aggregatedSequenceOutput: String = null
  @BeanProperty var rnaseqOutput: String = null
  @BeanProperty var featurizedOutput: String = null


  /* location of sequence motif data */
  @BeanProperty var deepbindPath: String = null


  /* These are required if createWindows is True */
  @BeanProperty var labels: String = null
  @BeanProperty var reference: String = null


  /* Not implemented data sources*/

  @BeanProperty var dnase: String = null
  @BeanProperty var rnaseq: String = null
  @BeanProperty var chipPeaks: String = null

  /* gene reference required for rnaseq location extraction */
  @BeanProperty var genes: String = null

  /* Featurization parameters */
  @BeanProperty var kmerLength: Int = 8
  @BeanProperty var sequenceLength: Int = 100
}

object EndiveConf {
  def validate(conf: EndiveConf) {
    /* Add line for required arugments here to validate 
     * TODO: This is a kludge but idk what else to do
     */

    if (conf.reference == null) {
      throw new IllegalArgumentException("Refrence path is mandatory")
    }
  }
}
