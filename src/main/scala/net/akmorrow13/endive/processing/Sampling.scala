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

import net.akmorrow13.endive.utils.LabeledWindow
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion


object Sampling {
  def selectNegativeSamples(sc: SparkContext, rdd: RDD[LabeledWindow], distance: Long = 700L): RDD[LabeledWindow] = {
    val minimum = 200L
    val positives = rdd.filter(r => r.label == 1.0)
    val negatives = rdd.filter(r => r.label == 0.0)
    val positivesB = sc.broadcast(positives.collect)

    // favor negative samples closer to positive samples
    val filteredNegs = negatives.filter(n => {
      val neighbors = positivesB.value.filter(r => {
        val dist =  n.win.region.distance(r.win.region)
        dist.isDefined && dist.get < distance && dist.get > minimum
      })
      !neighbors.isEmpty
    })

    filteredNegs.union(positives)
  }
}

