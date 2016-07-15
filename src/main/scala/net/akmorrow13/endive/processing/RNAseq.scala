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
package net.akmorrow13.endive.processing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion

class RNAseq(geneReference: String, @transient sc: SparkContext) {

  val genes = Preprocess.loadTranscripts(sc, geneReference)

  /**
   * Extracts sequence from a referenceRegion
   * @param rna RDD of records parsed from RNAseq tsv file
   */
  def extractGeneLocations(rna: RDD[RNARecord]): RDD[(ReferenceRegion, RNARecord)] = {
    val genesB = sc.broadcast(genes.collect.toList)
    rna.flatMap(r => {
      val g = genesB.value.filter(p => p.geneId == r.geneId).head
      g.transcripts.zip(g.regions)
        .filter(t => r.transcriptId.split(",").contains(t._1))
        .map(t => (t._2, RNARecord(r.geneId, t._1, r.length, r.effective_length, r.expected_count, r.TPM, r.FPKM)))
    })
  }
}

