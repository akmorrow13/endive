/**
 * Copyright 2015 Frank Austin Nothaft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.akmorrow13.endive.pipelines

import breeze.linalg.DenseVector
import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.featurizers.Motif
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, ReferencePosition, SequenceRecord, SequenceDictionary}
import org.bdgenomics.adam.rdd.read.AlignedReadRDD
import org.bdgenomics.formats.avro.{AlignmentRecord, Strand}

class VectorizedDnaseSuite extends EndiveFunSuite {
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  var motifPath = resourcePath("models.yaml")

  val chr = "chr1"
  val sd = new SequenceDictionary(Vector(SequenceRecord(chr, 7000)))

  sparkTest("msCentipede at full scale") {
    val scale = Some(0)
    val window = sc.parallelize(Seq(DenseVector(1,1,1,1,1,1,1,1)))
    val result = Dnase.centipedeRDD(window)
    val first = result.first
    assert(first(0) == 8.0)
  }

  sparkTest("test recentering based on motifs in featurizer") {
    val (labels, cellTypes) = Preprocess.loadLabels(sc, labelPath)
    val cellType = cellTypes.head
    val chr = labels.first._3.referenceName
    val dnase = List(PeakRecord(ReferenceRegion(chr, 2000,2010),1,1.0,1.0,1.0,1.0))
    val rdd = labels
      .filter(_._4 > 0)
      .map(r => LabeledWindow(Window(r._1, r._2, r._3, ("A" * 192 + "TTTAATTG"), Some(dnase)), r._4))

    val sd = new SequenceDictionary(Vector(SequenceRecord(chr, 100000)))
    val reads = sc.parallelize((1000 until 3000).map(r => {
      AlignmentRecord.newBuilder()
        .setStart(r.toLong)
        .setEnd(r.toLong + 1)
        .setContigName(chr)
        .setAttributes(cellType + ":file")
        .build()
    }))

    val coverage = new AlignedReadRDD(reads, sd, null)
    val motifs = Motif.parseYamlMotifs(motifPath)

    val results = VectorizedDnase.featurize(sc, rdd, coverage, sd, false, false,
                  Some(motifs), false)
    val features = results.first.features
    val featureLength = features.length
    assert(features.slice(features.length/2, features.length).sum == 0)
  }


}
