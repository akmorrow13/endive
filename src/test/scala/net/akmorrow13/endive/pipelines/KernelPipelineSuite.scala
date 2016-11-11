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

import com.google.common.io.Files
import net.akmorrow13.endive.{EndiveConf, EndiveFunSuite}
import org.bdgenomics.adam.rdd.ADAMContext._

class KernelPipelineSuite extends EndiveFunSuite {
  var sequences = resourcePath("EGR1_30")

  sparkTest("should save run with dnase and output to feature files") {

    val outputDir = Files.createTempDir()
    val outputFile = outputDir.getAbsolutePath + "/predictedFeatures.adam"

    // set configuration files
    val conf: EndiveConf = new EndiveConf()
    conf.setAggregatedSequenceOutput(sequences)
    conf.setReference("fakefile.2bit")
    conf.setSaveTrainPredictions(outputFile)

    // run pipeline
    KernelPipeline.run(sc, conf)

    // load back in training predictions and verify results
    val features = sc.loadFeatures(outputFile)
    assert(features.rdd.count == 8)
  }


}
