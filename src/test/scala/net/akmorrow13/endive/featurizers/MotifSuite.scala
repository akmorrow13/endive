package net.akmorrow13.endive.featurizers

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.{Dataset, Preprocess, Sequence}
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}

class MotifSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  var deepbindPath = "file:///Users/akmorrow/ADAM/endive/workfiles/deepbind"

  sparkTest("deepbind") {
    val labels: RDD[(String, String, ReferenceRegion, Int)] = Preprocess.loadLabels(sc, labelPath)

    // extract sequences from reference over training regions
    val sequences: RDD[LabeledWindow] =
      labels.map(r => LabeledWindow(Window(r._1, r._2, r._3, "ATGCG" * 40, List(), List()), r._4))
    val motif = new Motif(sc, deepbindPath)

    motif.scoreSequences(Dataset.tfs, sequences.map(_.win.sequence))

  }

}
