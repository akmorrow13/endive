package net.akmorrow13.endive

import net.akmorrow13.endive.processing.{CellTypes, Chromosomes, Dataset}

import scala.reflect.{BeanProperty, ClassTag}

class EndiveConf extends Serializable {
  @BeanProperty var createWindows: Boolean = false
  /* These are required if createWindows is False */

  @BeanProperty var windowLoc: String = null
  @BeanProperty var sequenceLoc: String = null
  @BeanProperty var rnaseqLoc: String = null
  @BeanProperty var dnaseLoc: String = null

  /* output files */
  @BeanProperty var aggregatedSequenceOutput: String = null
  @BeanProperty var rnaseqOutput: String = null
  @BeanProperty var featurizedOutput: String = null


  /* location of sequence motif data */
  @BeanProperty var deepbindPath: String = null
  @BeanProperty var motifDBPath: String = null

  /* These are required if createWindows is True */
  @BeanProperty var labels: String = null
  @BeanProperty var reference: String = null

  /* Cross validation parameteres */
  @BeanProperty var folds: Int = 2
  @BeanProperty var heldoutChr: Int = 1
  @BeanProperty var heldOutCells: Int = 1


  /* data sources*/
  @BeanProperty var labelsPathArray: Array[String] = Array()
  @BeanProperty var cutmapInputPath: String = null
  @BeanProperty var cutmapOutputPath: String = null
  @BeanProperty var predictionOutputPath: String = null
  @BeanProperty var modelTest: String = "true"
  /* dnase data */
  @BeanProperty var dnase: String = null
  @BeanProperty var useRawDnase: Boolean = false
  // location to RDD for list of (region, counts from files) for forward strands
  @BeanProperty var dnasePositives: String = null
  // location to RDD for list of (region, counts from files) for backward strands
  @BeanProperty var dnaseNegatives: String = null

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

    if (conf.heldoutChr > Chromosomes.toVector.length - 1) {
      throw new IllegalArgumentException("chrPerFold must be less than 23")
    }

    if (conf.heldOutCells > CellTypes.toVector.length - 1) {
      throw new IllegalArgumentException("chrPerFold must be less than 23")
    }

  }
}
