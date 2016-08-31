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

import net.akmorrow13.endive.EndiveFunSuite
import net.akmorrow13.endive.featurizers.Motif
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferenceRegion, ReferencePosition, SequenceRecord, SequenceDictionary}
import org.bdgenomics.formats.avro.Strand

class VectorizedDnaseSuite extends EndiveFunSuite {
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")
  var motifPath = resourcePath("models.yaml")


//  sparkTest("should merge dnase with labeled windows") {
//      val (labels, cellType) = Preprocess.loadLabels(sc, labelPath)
//      val rdd = labels.map(r => LabeledWindow(Window(r._1, r._2, r._3, "N" * 200), r._4))
//      val sd = new SequenceDictionary(Vector(SequenceRecord("chr10", 100000)))
//      val coverage = rdd.map(r => {
//        val countMap = Map(r.win.cellType -> 1)
//        CutMap(ReferencePosition(r.win.region.referenceName, r.win.region.start), countMap)
//      })
//
//      val baseFeatures = VectorizedDnase.featurize(sc, rdd, coverage, sd,  None, false)
//      assert(baseFeatures.count == 29)
//  }
//
//
//  sparkTest("msCentipede scale 0") {
//    val scale = Some(0)
//    val window = sc.parallelize(Seq(Array(1,1,1,1,1,1,1,1)))
//    val result = Dnase.centipedeRDD(window, scale)
//    val first = result.first
//    assert(first.length == 1)
//    assert(first.head == 8.0)
//  }
//
//  sparkTest("msCentipede scale 3") {
//    //    def msCentipede(windows: RDD[Array[Int]], scale: Option[Int])  = {
//    val window = sc.parallelize(Seq(Array(1,2,3,4,3,2,1,1)))
//    val result = Dnase.centipedeRDD(window)
//    val first = result.first
//    assert(first.length == 8)
//    assert(first.head == 17.0)
//    assert(first(1) == 10.0/17)
//    assert(first(2) == 3.0/10)
//  }
//
//  sparkTest("msCentipede at full scale") {
//    val scale = Some(0)
//    val window = sc.parallelize(Seq(Array(1,1,1,1,1,1,1,1)))
//    val result = Dnase.centipedeRDD(window, scale)
//    val first = result.first
//    assert(first.length == 1)
//    assert(first.head == 8.0)
//  }

  sparkTest("test recentering based on motifs in featurizer") {
    val (labels, cellTypes) = Preprocess.loadLabels(sc, labelPath)
    val cellType = cellTypes.head
    val chr = labels.first._3.referenceName
    val dnase = List(PeakRecord(ReferenceRegion(chr, 2000,2010),1,1.0,1.0,1.0,1.0))
    val rdd = labels
      .filter(_._4 > 0)
      .map(r => LabeledWindow(Window(r._1, r._2, r._3, ("A" * 192 + "TTTAATTG"), Some(dnase)), r._4))

    val sd = new SequenceDictionary(Vector(SequenceRecord(chr, 100000)))
    val series = (1000 until 3000).map(r => (cellType, r)).toSeq
    val coverage = sc.parallelize(series).map(r => {
      val countMap = Map(r._1 -> (r._2 % 5))
      CutMap(ReferencePosition(chr, r._2, Strand.FORWARD), countMap)
    })

    val motifs = Motif.parseYamlMotifs(motifPath)

    val results = VectorizedDnase.featurize(sc, rdd, coverage, sd, None, false,
                  Some(motifs))
    val features = results.first.features
    val featureLength = features.length
    assert(features.slice(features.length/2, features.length).sum == 0)
  }


}
