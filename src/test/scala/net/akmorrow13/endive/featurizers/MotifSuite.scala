package net.akmorrow13.endive.featurizers

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.processing.Dataset.TranscriptionFactors
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary, ReferenceRegion}
import org.bdgenomics.formats.avro.{Contig, NucleotideContigFragment}

class MotifSuite extends EndiveFunSuite {

  // training data of region and labels
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  var deepbindPath = "/Users/akmorrow/ADAM/endive/workfiles/deepbind"
  var motifPath = resourcePath("models.yaml")

  val sd = getSequenceDictionary

  sparkTest("deepbind for a small RDD of sequences") {
    // extract sequences from reference over training regions
    val sequences: RDD[String] = sc.parallelize(Seq("AGGUAAUAAUUUGCAUGAAAUAACUUGGAGAGGAUAGC"))
    val motif = new MotifScorer(sc, sd)

    val tfs = TranscriptionFactors.values.toList
    val results = motif.getDeepBindScores(sequences, tfs, deepbindPath)
  }

  test("should read pwms from yaml file") {
    val motifs = Motif.parseYamlMotifs(motifPath)
    assert(motifs.length == 3)
    val first = motifs.head.pwm
    assert(first(0) == 0.19882676005363464 && first(4) == 0.1623602658510208)
  }

}
