/**
 * Copyright 2015 Frank Austin Nothaft
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
package net.akmorrow13.endive

import net.akmorrow13.endive.preprocessing.Sequence
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{Argument, Option => Args4jOption}

object Endive extends BDGCommandCompanion {
  val commandName = "endive"
  val commandDescription = "computational methods for sequences and epigenomic datasets"

  def apply(cmdLine: Array[String]) = {
    new Endive(Args4j[EndiveArgs](cmdLine))
  }
}

class EndiveArgs extends Args4jBase {
  @Argument(required = true, metaVar = "REFERENCE", usage = "A 2bit file for the reference genome.", index = 0)
  var reference: String = null
  @Argument(required = true, metaVar = "CHIPSEQ", usage = "Peak file for CHIPSEQ", index = 1)
  var chipPeaks: String = null
@Args4jOption(required = false, name = "-kmerLength", usage = "kmer length")
  var kmerLength: Int = 8
  @Args4jOption(required = false, name = "-sequenceLength", usage = "sequence length around peaks")
  var sequenceLength: Int = 100
}

class Endive(protected val args: EndiveArgs) extends BDGSparkCommand[EndiveArgs] {
  val companion = Endive

  def run(sc: SparkContext) {

    // create new sequence with reference path
    val sequence = new Sequence(args.reference, sc)

    // get chip seq peaks
    val chipPeaks: RDD[Feature] = loadFeatures(sc, args.chipPeaks)

    // get list of kmers for all peaks around center of peak
    // results in SequenceSet: RDD[SparseVector], RDD[Double]
    var features = sequence.extractSequences(chipPeaks, args.sequenceLength, args.kmerLength)

    // regress on features

  }


  /**
   * Loads bed file over optional region
   * @param sc SparkContext
   * @param featurePath Feature path to load
   * @param region Optional region to load features from
   * @return RDD of Features
   */
  def loadFeatures(sc: SparkContext, featurePath: String, region: Option[ReferenceRegion] = None): RDD[Feature] = {
    val predicate =  Some((BinaryColumn("contig.contigName") === region.get.referenceName))

      if (featurePath.endsWith(".adam")) sc.loadParquetFeatures(featurePath, predicate)
    else if (featurePath.toLowerCase.endsWith("bed")) sc.loadFeatures(featurePath)
    else throw new Exception("File type not supported")
  }

}
