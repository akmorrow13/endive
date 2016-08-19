package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite

class RNAseqSuite extends EndiveFunSuite {

  // training data of region and labels
  var rnaPath = resourcePath("gene_expression.A549.biorep1_head10.tsv")
  var genePath = resourcePath("geneAnnotations_head50.gtf")

  sparkTest("should extract RNA from tsv file") {
    val rnaseq =  new RNAseq(genePath, sc)
    val trainRDD = rnaseq.loadRNA(sc, rnaPath)
    assert(trainRDD.count == 6)
  }

  sparkTest("should extract genes from gtf file") {
    val genes = Preprocess.loadTranscripts(sc, genePath)
    assert(genes.count == 27) // 62 transcripts in file
  }

}
