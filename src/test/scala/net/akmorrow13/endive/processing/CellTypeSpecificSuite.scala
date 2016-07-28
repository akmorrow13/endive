package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.pipelines.DatasetCreationPipeline
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary, ReferenceRegion}

class CellTypeSpecificSuite extends EndiveFunSuite {

  // training data of region and labels
  var peakPath = resourcePath("DNASE.A549.conservative.head30.narrowPeak")
  var rnaPath = resourcePath("gene_expression.A549.biorep1_head10.tsv")
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  var genePath = resourcePath("geneAnnotations_head50.gtf")

  var windowSize = 200
  val stride = 50

  sparkTest("should merge dnase, rnaseq and labels") {
    val dnaseRDD = Preprocess.loadPeaks(sc, peakPath)
    val labels: RDD[(String, String, ReferenceRegion, Int)] = Preprocess.loadLabels(sc, labelPath)
    val rnaseq =  new RNAseq(genePath, sc)
    val rnaseqRDD = rnaseq.loadRNA(sc, rnaPath)

    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] =
      labels.map(r => LabeledWindow(Window(r._1, r._2, r._3, "ATGCG" * 40, List(), List()), r._4))

    val sd = new SequenceDictionary(Vector(SequenceRecord("chr1", 2000L),
      SequenceRecord("chr2", 67402609),
      SequenceRecord("chr3", 245882740),
      SequenceRecord("chr5", 272192031),
      SequenceRecord("chr6", 42938256),
      SequenceRecord("chr7", 234050773),
      SequenceRecord("chr8", 76457025),
      SequenceRecord("chr9", 95666353),
      SequenceRecord("chr10", 2008762L),
      SequenceRecord("chr11", 86482433),
      SequenceRecord("chr12", 63265759),
      SequenceRecord("chr15", 75596394),
      SequenceRecord("chr17", 8060545),
      SequenceRecord("chr18", 66457025),
      SequenceRecord("chr19", 33962480),
      SequenceRecord("chr20", 62725155)))


    val cellTypeInfo = new CellTypeSpecific(windowSize,stride,dnaseRDD, rnaseqRDD, sd)
    val fullMatrix: RDD[LabeledWindow] = cellTypeInfo.joinWithSequences(sequences)
    assert(fullMatrix.count == 29)
  }
}
