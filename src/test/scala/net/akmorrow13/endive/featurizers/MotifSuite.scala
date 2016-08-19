package net.akmorrow13.endive.featurizers

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.{TranscriptionFactors, Dataset, Preprocess, Sequence}
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary, ReferenceRegion}
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}

class MotifSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  var deepbindPath = "/Users/akmorrow/ADAM/endive/workfiles/deepbind"
  val sd = getSequenceDictionary

  sparkTest("deepbind for a small RDD of sequences") {
    // extract sequences from reference over training regions
    val sequences: RDD[String] = sc.parallelize(Seq("AGGUAAUAAUUUGCAUGAAAUAACUUGGAGAGGAUAGC"))
    val motif = new Motif(sc, sd)

    val tfs = TranscriptionFactors.values.toList
    val results = motif.getDeepBindScores(sequences, tfs, deepbindPath)
  }

}
