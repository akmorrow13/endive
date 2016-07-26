package net.akmorrow13.endive.processing

import net.akmorrow13.endive.utils.LabeledWindow
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.SequenceDictionary


class DataSet(rdd: RDD[LabeledWindow], seed: Int = 100) {

  val r = new scala.util.Random(seed)
  val heldoutChr = chrs(r.nextInt(chrs.length))
  val heldoutCellType = cellTypes(r.nextInt(cellTypes.length))

  val train = rdd.filter(r => r.win.region.referenceName != heldoutChr && r.win.cellType != heldoutCellType)
  val test = rdd.filter(r => r.win.region.referenceName == heldoutChr || r.win.cellType == heldoutCellType)

  val cellTypes = List("A549","GM12878", "H1-hESC", "HCT116", "HeLa-S3", "HepG2", "IMR90", "K562",
    "MCF-7", "PC-3", "Panc1", "SK-N-SH", "induced_pluripotent_stem_cell", "liver")

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
    "chr22",
    "chr2",
    "chr3",
    "chr4",
    "chr5",
    "chr6",
    "chr7",
    "chr9",
    "chrX")

}



