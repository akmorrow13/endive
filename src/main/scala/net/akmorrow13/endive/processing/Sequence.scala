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

import nodes.nlp.{NGramsFeaturizer, Tokenizer}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.{Contig, Feature, NucleotideContigFragment}
import nodes.util.SparseFeatureVectorizer

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class Sequence(referencePath: String, @transient sc: SparkContext) {

  // Load reference Path
  val reference = sc.loadReferenceFile(referencePath, 10000)

  /**
   * Extracts relevent seqeunces surrounding feature points
   * @param points
   */
  def extractSequences(points: RDD[Feature], sequenceSize: Int, kmerSize: Int): RDD[(Feature, NucleotideContigFragment)] = {
    // used to get middle of pek sized at the sequence size
    // TODO: broadcast reference
    val r = reference.extract(ReferenceRegion("chr20", 0, 63025520)).toUpperCase()
    val half = sequenceSize / 2

    val features =
      points.map(f => {
        val middle = (f.getEnd - f.getStart)/2 + f.getStart
        val region = ReferenceRegion(f.getContigName, middle - half, middle + half)
        val s = try {
          val sequence = r.substring(region.start.toInt, region.end.toInt)
          // build NucleotideContigFragment from extracted sequence
          NucleotideContigFragment.newBuilder()
            .setFragmentSequence(sequence)
            .setFragmentEndPosition(region.end)
            .setFragmentStartPosition(region.start)
            .setFragmentLength(sequence.length.toLong)
            .setContig(Contig.newBuilder().setContigName(region.referenceName).setContigLength(sequence.length.toLong).build)
            .build
        }
        (f, s)
      })
    features.filter(r => !r._2.getFragmentSequence.contains('N'))
  }

}

object Sequence {

  def extractKmers(rdd: RDD[(Feature, NucleotideContigFragment)], kmerLength: Int): SequenceSet[SparseVector, Double] = {
    // extract sequences
    val sequences = rdd.map(_._2.getFragmentSequence)

    // featurize sequences to 8mers
    val featurizer = Tokenizer("") andThen NGramsFeaturizer[String](Seq(8))

    // extract all 8mers in each training point
    val results: RDD[Seq[String]] = featurizer(sequences).map(s => s.map(r => r.seq.reduce((x,y) => (x + y))))

    // count all kmers
    val vectors = countKmers(results, kmerLength)

    // map labels and vectors
    SequenceSet[SparseVector, Double](vectors, rdd.map(_._1.getScore))

  }

  def countKmers(rdd: RDD[Seq[String]], kmerLength: Int): RDD[SparseVector] = {
    val kmers = generateAllKmers(kmerLength) // gets all possible kmers of this length

    val vectors = rdd.map(s => {
                     val kmerMap = kmers.map(r => (r, 0)).toMap
                      s.foreach(f => kmerMap(f) += 1 )
                    kmerMap.values
                  })

    // TODO: map to sparse vector
    vectors.map(v => new SparseVector(v.map(_.toDouble).toArray))
  }

  def generateAllKmers(k: Int): List[String] = {
    val bases = Array('A','T','G','C')

    val kmers: ListBuffer[String] = new ListBuffer[String]()

    // TODO get all kmers
    kmers.toList

  }
}

case class SequenceSet[S: ClassTag, T: ClassTag](features: RDD[S], labels: RDD[T])
