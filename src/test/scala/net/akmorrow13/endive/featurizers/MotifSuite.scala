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
  var deepbindPath = "/Users/akmorrow/ADAM/endive/workfiles/deepbind"

  sparkTest("deepbind for a small RDD of sequences") {
    // extract sequences from reference over training regions
    val sequences: RDD[String] = sc.parallelize(Seq("AGGUAAUAAUUUGCAUGAAAUAACUUGGAGAGGAUAGC"))
    val motif = new Motif(sc, deepbindPath)

    val tfs = Dataset.tfs
    val results = motif.getDeepBindScores(sequences, tfs).collect
    println(results)

  }

}
