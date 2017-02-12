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
  @BeanProperty var numItersHardNegative: Int = 4

  /* Save predictions */
  @BeanProperty var saveTrainPredictions: String = null
  @BeanProperty var saveValPredictions: String = null

  /* Default prediction parameters for block solve */
  @BeanProperty var lambda: Double = 10000
  @BeanProperty var epochs: Int = 1

  @BeanProperty var sample: Boolean = true

  /* ICML/DEEPSEA config parameters */
  @BeanProperty var deepSeaTfs: Array[String] = EndiveConf.allDeepSeaTfs
  @BeanProperty var deepSeaDataPath: String = "deepsea_data/"
  @BeanProperty var expName: String = "icmlExperiments"

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

  @BeanProperty var modelBlockSize: Int = 256
}

object EndiveConf {
  def validate(conf: EndiveConf) {
    /* Add line for required arugments here to validate
     * TODO: This is a kludge but idk what else to do
     */

    if (conf.heldoutChr > Chromosomes.toVector.length - 1) {
      throw new IllegalArgumentException("chrPerFold must be less than 23")
    }

    if (conf.heldOutCells > CellTypes.toVector.length - 1) {
      throw new IllegalArgumentException("chrPerFold must be less than 23")
    }

  }
   val allDeepSeaTfs = Array("HSF1", "Pol2-4H8", "Rad21", "CCNT2", "Pol3", "Pol2", "BHLHE40", "SIX5", "BCLAF1", "GATA3", "ZNF263", "SETDB1", "BRCA1", "TRIM28", "GR", "SP1", "SP2", "SP4", "CREB1", "MafK", "PLU1", "GATA2", "Pbx3", "MafF", "Znf143", "GTF2F1", "GATA-1", "GATA-2", "RFX5", "NFKB", "RBBP5", "FOXA1", "RUNX3", "UBF", "FOXA2", "USF-1", "ELK4", "TCF12", "SIN3A", "CTCF", "FOXP2", "BATF", "ZNF217", "MAZ", "Brg1", "NELFe", "ERalpha", "TCF7L2", "Pol2(phosphoS2)", "JARID1A", "PAX5-C20", "SIRT6", "CHD1", "ZKSCAN1", "CHD2", "SAP30", "IKZF1", "ARID3A", "GTF2B", "ELK1", "ZBTB33", "TR4", "ZZZ3", "TCF3", "ZNF274", "NF-E2", "eGFP-FOS", "AP-2gamma", "STAT3", "BRF1", "ZEB1", "TAF7", "GRp20", "TAF1", "Pol2(b)", "TBP", "HNF4G", "HNF4A", "GABP", "MEF2C", "MEF2A", "NRSF", "PRDM1", "PHF8", "p300", "HDAC1", "HDAC2", "YY1", "HDAC6", "SPT20", "Bach1", "eGFP-JunB", "JunD", "BRF2", "FOXM1", "TFIIIC-110", "HMGN3", "AP-2alpha", "CBX3", "PAX5-N19", "NANOG", "STAT2", "STAT1", "NA", "SMC3", "NR2F2", "USF2", "USF1", "FOSL1", "FOSL2", "PML", "BAF155", "Nrf1", "IRF3", "SRF", "Sin3Ak-20", "COREST", "E2F6", "BDP1", "RXRA", "IRF1", "WHIP", "eGFP-HDAC8", "KAP1", "IRF4", "STAT5A", "PU.1", "NFATC1", "ERRA", "BCL11A", "EZH2", "ELF1", "TAL1", "CtBP2", "MYBL2", "c-Jun", "TEAD4", "CEBPB", "NF-YB", "CEBPD", "Max", "UBTF", "EGR1", "eGFP-GATA2", "ETS1", "SUZ12", "BAF170", "RPC155", "MTA3", "ZBTB7A", "c-Myc", "PGC1A", "POU5F1", "EBF1", "Ini1", "TBLR1", "NFIC", "THAP1", "NF-YA", "E2F4", "c-Fos", "E2F1", "ATF1", "CTCFL", "MBD4", "eGFP-JunD", "ATF3", "Mxi1", "HA-E2F1", "POU2F2", "BCL3", "ATF2")

}
