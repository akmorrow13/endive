package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite
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

  val sd = getSequenceDictionary

  sparkTest("should correctly window data overlapping windows") {
    // overlaps (0-200), (50-250), 100-300), (150-250), (200-400)
    val region1 = ReferenceRegion("chr1", 190, 210)
    // overlaps (0-200), (50-250), 100-300), (150-250), (200-400), 250-450)
    val region2 = ReferenceRegion("chr1", 140, 260)
    // overlaps (0,200), (50,250), 100,300), (150,250)
    val region3 = ReferenceRegion("chr1", 160, 190)

    val peakRecord1 = PeakRecord(region1, 1, 1.0, 1.0, 1.0, 1.0)
    val peakRecord2 = PeakRecord(region2, 1, 1.0, 1.0, 1.0, 1.0)
    val peakRecord3 = PeakRecord(region3, 1, 1.0, 1.0, 1.0, 1.0)

    val cellType  = CellTypes.A549.toString

    val dnase: RDD[(ReferenceRegion, String, PeakRecord)] =
        sc.parallelize(Seq((region1, cellType, peakRecord1),
          (region2, cellType, peakRecord2),
          (region3, cellType, peakRecord3)))

    val windowed = CellTypeSpecific.window(dnase, sd)
    assert(windowed.count == 6)
    assert(windowed.filter(_._1._1.start == 0).map(_._2).first.size == 3)
    assert(windowed.filter(_._1._1.start == 50).map(_._2).first.size == 3)
    assert(windowed.filter(_._1._1.start == 100).map(_._2).first.size == 3)
    assert(windowed.filter(_._1._1.start == 150).map(_._2).first.size == 3)
    assert(windowed.filter(_._1._1.start == 200).map(_._2).first.size == 2)
    assert(windowed.filter(_._1._1.start == 250).map(_._2).first.size == 1)
  }

  sparkTest("should merge dnase, rnaseq and labels") {
    val dnaseRDD = Preprocess.loadPeaks(sc, peakPath)
    val labels = Preprocess.loadLabels(sc, labelPath)._1
    val rnaseq =  new RNAseq(genePath, sc)
    val rnaseqRDD = rnaseq.loadRNA(sc, rnaPath)

    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] =
      labels.map(r => LabeledWindow(Window(r._1, r._2, r._3, "ATGCG" * 40, None, None), r._4))

    val sd = new SequenceDictionary(Chromosomes.toVector.map(r => SequenceRecord(r, 10000000)))

    val cellTypeInfo = new CellTypeSpecific(windowSize,stride,dnaseRDD, rnaseqRDD, sd)
    val fullMatrix: RDD[LabeledWindow] = cellTypeInfo.joinWithSequences(sequences)
    assert(fullMatrix.count == 29)
  }
}
