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
                       partition: Boolean = true,
                       partitioner: Vector[String] = Dataset.cellTypes.toVector): RDD[LabeledWindow] = {
    val partitionedRDD: RDD[((ReferenceRegion, String), LabeledWindow)] =
      if (partition)
        rdd.keyBy(r => (r.win.getRegion, r.win.getTf)).partitionBy(new LabeledReferenceRegionPartitioner(sd, partitioner))
      else rdd.keyBy(r => (r.win.getRegion, r.win.getTf))

    partitionedRDD.mapPartitions(iter => {
      val sites = iter.toList
      val positives = sites.filter(r => r._2.label == 1.0)
      val negatives = sites.filter(r => r._2.label == 0.0)
      val minimum = 200L

      // favor negative samples closer to positive samples with open chromatin
      val filteredNegs = negatives.filter(n => {
        val neighbors = positives.filter(p => {
          val dist =  n._2.win.getRegion.distance(p._2.win.getRegion)
          dist.isDefined && dist.get < distance && dist.get > minimum && p._2.win.getTf == n._2.win.getTf && p._2.win.getCellType == n._2.win.getCellType
        })
        !neighbors.isEmpty
      })
      filteredNegs.union(positives).toIterator
    }).map(_._2)
  }
}

