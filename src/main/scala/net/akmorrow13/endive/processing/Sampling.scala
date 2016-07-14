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

import nodes.nlp.{NGramsFeaturizer, Tokenizer}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{ReferenceContigMap, ReferenceFile}
import org.bdgenomics.formats.avro.NucleotideContigFragment

import scala.reflect.ClassTag

object Sampling {
  def selectNegativeSamples(sc: SparkContext, rdd: RDD[(ReferenceRegion, Double)], distance: Long = 700L): RDD[(ReferenceRegion, Double)] = {
    val minimum = 200L
    val positives = rdd.filter(r => r._2 == 1.0)
    val negatives = rdd.filter(r => r._2 == 0.0)
    val positivesB = sc.broadcast(positives.collect)

    // favor negative samples closer to positive samples
    val filteredNegs = negatives.filter(n => {
      val neighbors = positivesB.value.filter(r => {
        val dist =  n._1.distance(r._1)
        dist.isDefined && dist.get < distance && dist.get > minimum
      })
      !neighbors.isEmpty
    })

    filteredNegs.union(positives)
  }
}

