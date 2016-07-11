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
package net.akmorrow13.endive.preprocessing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.NucleotideContigFragment

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class Sequence(referencePath: String, @transient sc: SparkContext) {

  // Load reference Path
  val reference: RDD[NucleotideContigFragment] =
    if (referencePath.endsWith(".fa") || referencePath.endsWith(".fasta"))
      sc.loadSequences(referencePath)
    else
      throw new IllegalArgumentException("Unsupported reference file format")

  /**
   * Extracts sequence from a referenceRegion
   * @param points
   */
  def extractSequences(points: RDD[(ReferenceRegion, Seq[Int])]): RDD[(ReferenceRegion, String, Seq[Int])] = {
    // used to get middle of pek sized at the sequence size
    // TODO: broadcast reference
    val parsedSequences: RDD[(ReferenceRegion, String, Seq[Int])] =
      points.map(r => {
        (r._1, reference.getReferenceString(r._1), r._2)
    })
    parsedSequences
  }
}

object Sequence {

//  def extractKmers(rdd: RDD[(Feature, NucleotideContigFragment)], kmerLength: Int): SequenceSet[SparseVector, Double] = {
//    // extract sequences
//    val sequences = rdd.map(_._2.getFragmentSequence)
//
//    // featurize sequences to 8mers
//    val featurizer = Tokenizer("") andThen NGramsFeaturizer[String](Seq(8))
//
//    // extract all 8mers in each training point
//    val results: RDD[Seq[String]] = featurizer(sequences).map(s => s.map(r => r.seq.reduce((x,y) => (x + y))))
//
//    // count all kmers
//    val vectors = countKmers(results, kmerLength)
//
//    // map labels and vectors
//    SequenceSet[SparseVector, Double](vectors, rdd.map(_._1.getScore))
//
//  }

//  def countKmers(rdd: RDD[Seq[String]], kmerLength: Int): RDD[SparseVector] = {
//    val kmers = generateAllKmers(kmerLength) // gets all possible kmers of this length
//
//    val vectors = rdd.map(s => {
//                     val kmerMap = kmers.map(r => (r, 0)).toMap
//                      s.foreach(f => kmerMap(f) += 1 )
//                    kmerMap.values
//                  })
//
//  }

  def generateAllKmers(k: Int): List[String] = {
    val bases = Array('A','T','G','C')

    val kmers: ListBuffer[String] = new ListBuffer[String]()

    // TODO get all kmers
    kmers.toList

  }
}

case class SequenceSet[S: ClassTag, T: ClassTag](features: RDD[S], labels: RDD[T])
