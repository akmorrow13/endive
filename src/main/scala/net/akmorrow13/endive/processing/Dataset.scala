package net.akmorrow13.endive.processing

import net.akmorrow13.endive.utils.LabeledWindow
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary

object Dataset {

  // held out values for final round
  val heldOutChrs = List("chr1", "chr8", "chr21")
  val heldOutTypes = List("PC-3", "liver", "induced_pluripotent_stem_cell")

  val cellTypes = List("A549","GM12878", "H1-hESC", "HCT116", "HeLaS3", "HepG2", "IMR90", "K562",
    "MCF7", "PC3",
    "Panc1", "SKNSH", "inducedpluripotentstemcell", "liver")

  val tfs = List("ARID3A",
    "CEBPB", "EGR1", "HNF4A", "REST", "TCF12",
    "EP300", "JUND",	"RFX5",	"TCF7L2",
    "CREB1",	"FOXA1",	"MAFK",	"SPI1",	"TEAD4",
    "ATF2",	"CTCF",	"FOXA2",	"MAX",		"SRF",		"YY1",
    "ATF3",	"E2F1",	"GABPA",	"MYC",		"STAT3",	"ZNF143",
    "ATF7",	"E2F6",	"GATA3",	"NANOG",	"TAF1")
  val chrs = List("chr10",
    "chr11",
    "chr12",
    "chr13",
    "chr14",
    "chr15",
    "chr16",
    "chr17",
    "chr18",
    "chr19",
    "chr1",
    "chr20",
    "chr21",
    "chr22",
    "chr2",
    "chr3",
    "chr4",
    "chr5",
    "chr6",
    "chr7",
    "chr8",
    "chr9",
    "chrX")

}



