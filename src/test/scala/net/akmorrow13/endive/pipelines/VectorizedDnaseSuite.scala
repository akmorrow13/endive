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
import net.akmorrow13.endive.processing.{Dnase, CutMap, Cut, Preprocess}
import net.akmorrow13.endive.utils.{Window, LabeledWindow}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, SequenceRecord, SequenceDictionary}

class VectorizedDnaseSuite extends EndiveFunSuite {
  var labelPath = resourcePath("ARID3A.train.labels.head30.tsv")

  sparkTest("should merge dnase with labeled windows") {
      val (labels, cellType) = Preprocess.loadLabels(sc, labelPath)
      val rdd = labels.map(r => LabeledWindow(Window(r._1, r._2, r._3, "N" * 200), r._4))
      val sd = new SequenceDictionary(Vector(SequenceRecord("chr10", 100000)))
      val coverage = rdd.map(r => {
        val countMap = Map(r.win.cellType -> 1)
        CutMap(ReferencePosition(r.win.region.referenceName, r.win.region.start), countMap)
      })

      val baseFeatures = VectorizedDnase.featurize(sc, rdd, coverage, sd,  None, false)
      assert(baseFeatures.count == 29)
  }


  sparkTest("msCentipede scale 0") {
    val scale = Some(0)
  //    def msCentipede(windows: RDD[Array[Int]], scale: Option[Int])  = {
    val window = sc.parallelize(Seq(Array(1,1,1,1,1,1,1,1)))
    val result = Dnase.centipedeRDD(window, scale)
    val first = result.first
    assert(first.length == 1)
    assert(first.head == 8.0)
  }

  sparkTest("msCentipede scale 3") {
    //    def msCentipede(windows: RDD[Array[Int]], scale: Option[Int])  = {
    val window = sc.parallelize(Seq(Array(1,2,3,4,3,2,1,1)))
    val result = Dnase.centipedeRDD(window)
    val first = result.first
    assert(first.length == 8)
    assert(first.head == 17.0)
    assert(first(1) == 10.0/17)
    assert(first(2) == 3.0/10)
  }
}
