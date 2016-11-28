/**
 * Copyright 2016 Alyssa Morrow
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

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.stats.distributions.Gaussian
import com.google.common.io.Files
import net.akmorrow13.endive.processing.{TranscriptionFactors, Dataset, CellTypes}
import net.akmorrow13.endive.utils.{Window, LabeledWindowLoader, LabeledWindow}
import net.akmorrow13.endive.{EndiveConf, EndiveFunSuite}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignedReadRDD
import org.bdgenomics.formats.avro.AlignmentRecord

class EndiveLearningPipelineSuite extends EndiveFunSuite {
  val modelPath = resourcePath("model.model")

  sparkTest("load model back in") {
    val regionAndSeq = (ReferenceRegion("chrM", 0, 100), "A"*100)
    val server = ModelServer(modelPath, 100)
    val soln = server.serve(regionAndSeq)
    assert(soln.length == 1)
  }

}
