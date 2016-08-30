/**
 * Copyright 2015 Vaishaal Shankar
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
package net.akmorrow13.endive.pipelines

import breeze.linalg._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.processing.Sequence
import net.akmorrow13.endive.utils._
import nodes.learning._
import nodes.nlp._
import nodes.stats.TermFrequency
import nodes.util.CommonSparseFeatures
import nodes.util.{Identity, Cacher, ClassLabelIndicatorsFromIntLabels, TopKClassifier, MaxClassifier, VectorCombiner}
import utils.{Image, MatrixUtils, Stats, ImageMetadata, LabeledImage, RowMajorArrayVectorizedImage, ChannelMajorArrayVectorizedImage}

import com.github.fommil.netlib.BLAS
import evaluation.BinaryClassifierEvaluator
import org.apache.log4j.{Level, Logger}
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.kohsuke.args4j.{Option => Args4jOption}
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging

object BlasTest extends Serializable with Logging {

  /**
   * Just print version of blas on master and executors
   *
   *
   * @param args
   */
  def main(args: Array[String]) = {
      val conf = new SparkConf().setAppName("BlasTest")
      conf.setIfMissing("spark.master", "local[4]")
      val sc = new SparkContext(conf)
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)
      val blasVersion = BLAS.getInstance().getClass().getName()
      println(s"Currently used version of blas (in driver) is ${blasVersion}")
      val blasVersionSlaves = sc.parallelize((0 until 100)).map { x => BLAS.getInstance().getClass().getName() }.collect().toSet.mkString(",")
      println(s"Currently used version of blas (in slaves) is ${blasVersionSlaves}")
    }
  }
