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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.TwoBitFile
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
    println(args.reference)
    println(args.chipPeaks)

  // extract reference from two bit file
  val reference: TwoBitFile = Sequence.load2Bit(args.reference)

  // get chip peaks
  val chipPeaks: RDD[Feature] = loadFeatures(sc, args.chipPeaks)

  // get list of kmers for all peaks around center of peak
  val sequences = Sequence.extractSequences(reference, chipPeaks, args.sequenceLength, args.kmerLength)
  println(sequences.count)

    // perform regression on sequences and peaks
  }



  def loadFeatures(sc: SparkContext, featurePath: String): RDD[Feature] = {
    if (featurePath.endsWith(".adam")) sc.loadParquetFeatures(featurePath)
    else if (featurePath.endsWith("bed")) sc.loadFeatures(featurePath)
    else throw new Exception("File type not supported")
  }

}
