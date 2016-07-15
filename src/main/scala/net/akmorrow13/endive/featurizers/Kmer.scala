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
package net.akmorrow13.endive.featurizers

import nodes.nlp.{NGramsFeaturizer, Tokenizer}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{ReferenceContigMap, ReferenceFile}
import org.bdgenomics.formats.avro.NucleotideContigFragment

object Kmer {
  def extractKmers(rdd: RDD[(ReferenceRegion, String)], kmerLength: Int): RDD[Vector] = {
    // extract sequences
    val sequences = rdd.map(_._2)

    // featurize sequences to 8mers
    val featurizer = Tokenizer("") andThen NGramsFeaturizer[String](Seq(kmerLength))

    // extract all 8mers in each training point
    val results: RDD[Seq[String]] = featurizer(sequences).get.map(s => s.map(r => r.seq.reduce((x,y) => (x + y))))

    // count all kmers
    countKmers(results, kmerLength)

  }

  def countKmers(rdd: RDD[Seq[String]], kmerLength: Int): RDD[Vector] = {
    val kmers = generateAllKmers(kmerLength) // gets all possible kmers of this length
    val kmerCount = kmers.length

    val vectors: RDD[Vector] = rdd.map(s => {
      val sparse: Seq[(Int, Double)] = s.groupBy(identity) // group by kmer
                                                .map(r => (r._1, r._2.length))  // count kmer occurances
                                                .map(r => (kmers.indexOf(r._1), r._2.toDouble)) // map to position in list of all kmers
                                                .toSeq
      Vectors.sparse(kmers.length, sparse)
    })

    vectors

  }

  def generateAllKmers(k: Int): Array[String] = {
    generateKmer("", k - 1)
  }

  def generateKmer(s: String, pos: Int): Array[String] = {
    val bases = Array('A','T','G','C')
    if (pos < 0) {
      return Array(s)
    }
    bases.flatMap(b => generateKmer(s + b, pos - 1))
  }
}
