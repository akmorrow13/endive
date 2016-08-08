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
package net.akmorrow13.endive.metrics

import net.akmorrow13.endive.utils.LabeledWindow
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics


object Metrics {

  def printMetrics(metrics: BinaryClassificationMetrics) {
    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
  }

  /**
   * Saves results as required DREAM format:
   * Column 1: Name of chromosome
   * Column 2: Start coordinate of bin (0-based i.e. assuming first coordinate in the chromosome is 0, e.g.: 600)
   * Column 3: Stop coordinate of bin (1-based i.e. assuming first coordinate in the chromosome is 1, e.g.: 800)
   * Column 4: Predicted TF binding probability (A real-value between 0 and 1)
   * @param rdd region and score between 0 and 1
   * @param path path to save results to
   *
   */
  def saveResults(rdd: RDD[(ReferenceRegion, Double)], path: String): Unit = {
    rdd.map(r => Seq(r._1.referenceName, r._1.start, r._1.end, r._2))
      .map(r => r.mkString("\t"))
      .saveAsTextFile(path)
  }
}

