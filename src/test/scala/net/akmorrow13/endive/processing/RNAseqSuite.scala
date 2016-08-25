package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite

class RNAseqSuite extends EndiveFunSuite {

  // training data of region and labels
  var rnaPath = resourcePath("gene_expression.A549.biorep1.tsv")
  var genePath = resourcePath("gencode.v19.annotation.gtf")

  sparkTest("should extract RNA from tsv file") {
    val rnaseq =  new RNAseq(genePath, sc)
    val trainRDD = rnaseq.loadRNA(sc, rnaPath)
    trainRDD.collect.foreach(println)
    assert(trainRDD.count == 196520)
  }

  sparkTest("should extract genes from gtf file") {
    val genes = Preprocess.loadTranscripts(sc, genePath)
    //println(genes)
    assert(genes.count == 196520) // 62 transcripts in file
  }

}

