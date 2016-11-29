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
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object Kmer {

  /**
   * Featurizes sequences into a sparse vector of kmer counts
   * @param sequences RDD of sequences to featurize
   * @param k length of kmers
   * @param maxDistance maximum distance to count kmers from
   * @return RDD of sparse vector of kmer counts
   */
  def extractKmers(sequences: RDD[String], k: Int, maxDistance: Int = 0): RDD[Vector] = {
    // featurize sequences to kmers
    val featurizer = Tokenizer("") andThen NGramsFeaturizer[String](Seq(k))

    // extract all 8mers in each training point
    val results: RDD[Seq[String]] = featurizer(sequences).map(s => s.map(r => r.seq.reduce((x,y) => (x + y))))

    // count all kmers
    countKmers(results, k, maxDistance)

  }

  /**
   * Counts the number of times each kmer occurs and stores resulting counts
   * in a sparse vector
   * @param rdd RDD of sequences of kmers
   * @param kmerLength Length of kmers in RDD
   * @return Vector represenatation of kmer counts
   */
  def countKmers(rdd: RDD[Seq[String]], kmerLength: Int): RDD[Vector] = {
    val kmers = generateKmers(kmerLength) // gets all possible kmers of this length
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

  /**
   * Counts the number of times each kmer occurs and stores resulting counts
   * in a sparse vector
   * @param rdd RDD of sequences of kmers
   * @param kmerLength Length of kmers in RDD
   * @param maxDistance threshold to count kmer differences from
   * @return Vector represenatation of kmer counts
   */
  def countKmers(rdd: RDD[Seq[String]], kmerLength: Int, maxDistance: Int): RDD[Vector] = {
    if (maxDistance == 0)
      return countKmers(rdd, kmerLength) // this implementation is more efficient
    val kmers = generateKmers(kmerLength) // gets all possible kmers of this length
    val kmerCount = kmers.length

    val vectors: RDD[Vector] = rdd.map(s => {
      val sparse: Seq[(Int, Double)] = kmers.zipWithIndex.map(k => {
        (k._2, s.filter(r => r.diff(k._1).length <= maxDistance).size.toDouble)
      }).filter(_._2 > 0.0)

      Vectors.sparse(kmers.length, sparse)
    })
    vectors
  }

  /**
   * Generates all possible kmers of length k
   * @param k length of kmer
   * @return Array of all possible enumerations of kmers
   */
  def generateKmers(k: Int): Array[String] = {
    generateKmer(k - 1)
  }

  /**
   * Recursively generates all kmers of length k
   * @param s Kmer string to iterate on
   * @param k length of kmer
   * @return Array of all kmers of length k
   */
  private def generateKmer(k: Int, s: String = ""): Array[String] = {
    val bases = Array('A','T','G','C')
    if (k < 0) {
      return Array(s)
    }
    bases.flatMap(b => generateKmer(k - 1, s + b))
  }

}
