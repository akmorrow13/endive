package net.akmorrow13.endive.processing

import org.bdgenomics.utils.misc.Logging

object Dataset extends Logging {

  def filterCellTypeName(cellType: String): String = {
    val newCellType = cellType.filterNot("-_".toSet)
    try {
      CellTypes.withName(newCellType)
    } catch {
      case e: NoSuchElementException => log.error("Error, celltype not found in list of available cell types")
    }
    newCellType
  }

  // window settings
  val windowSize = 200
  val stride = 20

  // held out values for final round
  val heldOutChrs = List(Chromosomes.chr1, Chromosomes.chr8, Chromosomes.chr21)
  val heldOutTypes = List(CellTypes.PC3, CellTypes.liver, CellTypes.inducedpluripotentstemcell)

}

object TranscriptionFactors extends Enumeration {
  val ARID3A, CEBPB, EGR1, HNF4A, REST, TCF12,
  EP300, JUND,	RFX5,	TCF7L2,
  CREB1,	FOXA1,	MAFK,	SPI1,	TEAD4,
  ATF2,	CTCF,	FOXA2,	MAX,		SRF,		YY1,
  ATF3,	E2F1,	GABPA,	MYC,		STAT3,	ZNF143,
  ATF7,	E2F6,	GATA3,	NANOG,	TAF1 = Value

  def toVector: Vector[String] = this.values.map(_.toString).toVector
}

object CellTypes extends Enumeration {
  val A549,GM12878, H1hESC, HCT116, HeLaS3, HepG2, IMR90, K562,
  MCF7, PC3,
  Panc1, SKNSH, inducedpluripotentstemcell, liver = Value

  def toVector: Vector[String] = this.values.map(_.toString).toVector

}

object Chromosomes extends Enumeration {
  val chr10, chr11, chr12, chr13, chr14, chr15, chr16,
  chr17, chr18, chr19, chr1, chr20, chr21, chr22, chr2,
  chr3, chr4, chr5, chr6, chr7, chr8, chr9, chrX = Value

  def toVector: Vector[String] = this.values.map(_.toString).toVector

}


