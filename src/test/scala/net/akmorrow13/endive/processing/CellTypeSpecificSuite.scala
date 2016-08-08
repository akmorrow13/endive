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
    val labels: RDD[(String, String, ReferenceRegion, Int)] = Preprocess.loadLabels(sc, labelPath)._1
    val rnaseq =  new RNAseq(genePath, sc)
    val rnaseqRDD = rnaseq.loadRNA(sc, rnaPath)

    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] =
      labels.map(r => LabeledWindow(Window(r._1, r._2, r._3, "ATGCG" * 40, None, None), r._4))

    val sd = new SequenceDictionary(Dataset.chrs.map(r => SequenceRecord(r, 10000000)).toVector)

    val cellTypeInfo = new CellTypeSpecific(windowSize,stride,dnaseRDD, rnaseqRDD, sd)
    val fullMatrix: RDD[LabeledWindow] = cellTypeInfo.joinWithSequences(sequences)
    assert(fullMatrix.count == 29)
  }
}
