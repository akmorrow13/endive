package net.akmorrow13.endive
import scala.reflect.{BeanProperty, ClassTag}

class EndiveConf extends Serializable {
  /* These are required */
  @BeanProperty var labels: String = null
  @BeanProperty var reference: String = null

  /* Not implemented data sources*/

  @BeanProperty var dnase: String = null
  @BeanProperty var rnase: String = null
  @BeanProperty var chipPeaks: String = null

  /* Featurization parameters */

  @BeanProperty var kmerLength: Int = 8
  @BeanProperty var sequenceLength: Int = 100
}

object EndiveConf {
  def validate(conf: EndiveConf) {
    /* Add line for required arugments here to validate 
     * TODO: This is a kludge but idk what else to do
     */

    if (conf.labels == null) {
      throw new IllegalArgumentException("Labels path is mandatory")
    }

    if (conf.reference == null) {
      throw new IllegalArgumentException("Refrence path is mandatory")
    }
  }
}
