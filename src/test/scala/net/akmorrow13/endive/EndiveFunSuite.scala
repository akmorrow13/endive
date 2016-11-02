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

import net.akmorrow13.endive.processing.Dataset.Chromosomes
import org.apache.commons.math3.genetics.Chromosome
import org.bdgenomics.adam.models.{SequenceRecord, SequenceDictionary}
import org.bdgenomics.utils.misc.SparkFunSuite

trait EndiveFunSuite extends SparkFunSuite {
  override val appName: String = "endive"
  override val properties: Map[String, String] = Map(("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
    ("spark.kryo.registrator", "net.akmorrow13.endive.EndiveKryoRegistrator"),
    ("spark.kryoserializer.buffer.mb", "4"),
    ("spark.kryo.referenceTracking", "true"),
    ("spark.executor.memory","32G"),
    ("spark.master.memory", "32G"))

  // fetches resources
  def resourcePath(path: String) = ClassLoader.getSystemClassLoader.getResource(path).getFile

  def getSequenceDictionary: SequenceDictionary = {
    val records = Chromosomes.toVector.map(r => SequenceRecord(r, 10000000))
    new SequenceDictionary(records)
  }
}
