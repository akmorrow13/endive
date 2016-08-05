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

import net.akmorrow13.endive.utils.{LabeledReferenceRegionPartitioner, LabeledWindow}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import org.bdgenomics.adam.rdd.GenomicPositionPartitioner


object Sampling {
  /**
   * Filters negative samples close to true peaks with open chromatin
   * @param sc
   * @param rdd
   * @param distance
   * @return
   */
  def subselectSamples(sc: SparkContext,
                       rdd: RDD[LabeledWindow],
                       sd: SequenceDictionary,
                       distance: Long = 700L,
                       partitioner: Vector[String] = Dataset.tfs.toVector): RDD[LabeledWindow] = {
    val partitionedRDD = rdd.keyBy(r => (r.win.getRegion, r.win.getTf)).partitionBy(new LabeledReferenceRegionPartitioner(sd, partitioner))

    partitionedRDD.mapPartitions(iter => {
      val sites = iter.toList
      val positives = sites.filter(r => r._2.label == 1.0)
      val negatives = sites.filter(r => r._2.label == 0.0)
              .filter( r => r._2.win.getDnase.length > 0)  // filter regions only with dnase
      val minimum = 200L

      // favor negative samples closer to positive samples with open chromatin
      val filteredNegs = negatives.filter(n => {
        val neighbors = positives.filter(p => {
          val dist =  n._2.win.getRegion.distance(p._2.win.getRegion)
          dist.isDefined && dist.get < distance && dist.get > minimum
        })
        !neighbors.isEmpty
      })
      filteredNegs.union(positives).toIterator
    })
    val minimum = 200L
    val positives = rdd.filter(r => r.label == 1.0)
    val negatives = rdd.filter(r => r.label == 0.0)
    val positivesB = sc.broadcast(positives.collect)

    // favor negative samples closer to positive samples
    val filteredNegs = negatives.filter(n => {
      val neighbors = positivesB.value.filter(r => {
        val dist =  n.win.getRegion.distance(r.win.getRegion)
        dist.isDefined && dist.get < distance && dist.get > minimum
      })
      !neighbors.isEmpty
    })
    println(s"final negatives with accessible chromatin around peaks count: ${filteredNegs.count}")

    filteredNegs.union(positives)
  }
}

