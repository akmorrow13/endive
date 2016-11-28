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
import com.google.common.io.Files
import net.akmorrow13.endive.processing.{TranscriptionFactors, Dataset, CellTypes}
import net.akmorrow13.endive.utils.{Window, LabeledWindowLoader, LabeledWindow}
import net.akmorrow13.endive.{EndiveConf, EndiveFunSuite}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignedReadRDD
import org.bdgenomics.formats.avro.AlignmentRecord

class KernelPipelineSuite extends EndiveFunSuite {
  var windowPath = resourcePath("EGR1_30")
  val referencePath = resourcePath("chr14chr6.2bit")

  sparkTest("should save run and output to feature files") {

    val outputDir = Files.createTempDir()
    val outputFile = outputDir.getAbsolutePath + "/predictedFeatures.adam"

    // set configuration files
    val conf: EndiveConf = new EndiveConf()
    conf.setAggregatedSequenceOutput(windowPath)
    conf.setReference(referencePath)
    conf.setDim(100)
    conf.setSample(false)
    conf.setSaveTrainPredictions(outputFile)

    // run pipeline
    KernelPipeline.run(sc, conf)

    // load back in training predictions and verify results
    val features = sc.loadFeatures(outputFile)
    assert(features.rdd.count == 7)
  }


  sparkTest("should featurize dnase and join with labeled windows") {
    val filteredRegion = ReferenceRegion("chr6", 32783200L, 32783500L)

    val windows: RDD[LabeledWindow] =
      LabeledWindowLoader(windowPath, sc)
      .filter(_.win.getRegion.overlaps(filteredRegion))

    val cells = windows.map(_.win.getCellType).distinct.collect

    // create dnase that overlaps a window
    val dnase = AlignmentRecord.newBuilder()
                .setAttributes("MCF7:file")
                .setContigName("chr6")
                .setStart(32783380L)
                .setEnd(32783381L)
                .setReadNegativeStrand(false)
                .build()

    val dnaseRDD = AlignedReadRDD(sc.parallelize(Seq(dnase)), getSequenceDictionary, null)

    val merged = KernelPipeline.mergeDnase(sc, windows, getSequenceDictionary, cells, dnaseRDD, false)
      .filter(_.labeledWindow.win.getRegion.overlaps(filteredRegion))
      .sortBy(_.labeledWindow.win.region)
      .collect

    // assert dnase was correctly inserted in vectors
    assert(merged.length == 2)
    assert(merged.head.features.sum > 0)
    assert(merged.head.features(180) == 1.0)

    assert(merged.last.features.sum > 0)
    assert(merged.last.features(30) == 1.0)

  }

  test("one hot encodes dnase with sequence") {
    val region = ReferenceRegion("chr6", 32783200L, 32783500L)
    val sequence = "ATCG"
    val dnase = DenseVector(0.0,9.0,10.0,3.0)
    val window = Window(TranscriptionFactors.ARID3A,
                    CellTypes.A549, region, sequence)
    val encoded = KernelPipeline.oneHotEncodeDnase(BaseFeature(LabeledWindow(window, 1), dnase))

    assert(encoded.length == sequence.length * Dataset.alphabet.size)
    assert(encoded(0) == 1)
    assert(encoded(5) == 10)
    assert(encoded(10) == 11)
    assert(encoded(15) == 4)
  }

  sparkTest("Test that we can run a pipeline from outside Endive") {
    val kernelPipeline = new KernelPipeline("/Users/DevinPetersohn/endive/confs/kernel.conf")
    println(kernelPipeline.prediction.take(10))
  }

}
