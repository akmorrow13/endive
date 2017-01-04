package net.akmorrow13.endive

import net.akmorrow13.endive.processing.{Chromosomes, CellTypes}

import scala.reflect.{BeanProperty, ClassTag}

class EndiveConf extends Serializable {
  /* for test */
  @BeanProperty var test: Boolean = false

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
  @BeanProperty var alphabetSize: Int = 4


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

  @BeanProperty var gamma: Double = 1.0
  @BeanProperty var seed: Int = 0
  @BeanProperty var readFiltersFromDisk: Boolean = false

  @BeanProperty var valDuringSolve: Boolean = true
  @BeanProperty var writePredictionsToDisk: Boolean = true
  @BeanProperty var predictionsOutput: String = "/user/vaishaal/tmp"
  @BeanProperty var featuresOutput: String = "/tmp"
  @BeanProperty var modelOutput: String = "/tmp"
  @BeanProperty var featurizeSample: Double = 1.0
  @BeanProperty var numPartitions: Int = 400
  @BeanProperty var negativeSamplingFreq: Double = 1.0
  @BeanProperty var filtersPath: String  = "/tmp/filters.csv"
  @BeanProperty var useDnase: Boolean = true

  /* train test split for solve */
  @BeanProperty var valChromosomes: Array[String] = Array()
  @BeanProperty var valCellTypes: Array[Int] = Array()


  /* data sources*/
  @BeanProperty var labelsPathArray: Array[String] = Array()
  @BeanProperty var cutmapInputPath: String = null
  @BeanProperty var cutmapOutputPath: String = null
  @BeanProperty var predictionOutputPath: String = null
  @BeanProperty var modelTest: String = "true"


  /* dnase data */
  @BeanProperty var dnaseNarrow: String = null
  @BeanProperty var dnaseBams: String = null
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
  @BeanProperty var dnaseKmerLength : Int = 100
  @BeanProperty var sequenceLength: Int = 100

  /* Kernel approximation feature parameters */
  @BeanProperty var approxDim: Int = 256
  @BeanProperty var mixtureWeight: Double = -1.0
  @BeanProperty var numItersHardNegative: Int = 10

  /* Save predictions */
  @BeanProperty var saveTrainPredictions: String = null
  @BeanProperty var saveValPredictions: String = null

  /* Default prediction parameters for block solve */
  @BeanProperty var lambda: Double = 10000
  @BeanProperty var epochs: Int = 1

  @BeanProperty var sample: Boolean = true

  /* Configuration values used for saving test set */

  // comma separated list of TFS (ie EGR1,ATF2...)
  @BeanProperty var tfs: String = null
  // comma separated list of cell types (ie K562,GM12878...)
  @BeanProperty var cellTypes: String = null
  @BeanProperty var hasSequences: Boolean = true

  @BeanProperty var saveTestPredictions: String = null

  // which board is being tested?
  @BeanProperty var ladderBoard: Boolean = false
  @BeanProperty var testBoard: Boolean = false

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
