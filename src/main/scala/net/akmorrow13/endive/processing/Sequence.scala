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

import java.io.{File, FileInputStream}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.util.{ReferenceContigMap, ReferenceFile, TwoBitFile}
import org.bdgenomics.formats.avro.NucleotideContigFragment
import org.bdgenomics.utils.io.{ByteArrayByteAccess, LocalFileByteAccess}

import scala.reflect.ClassTag

class Sequence(reference: ReferenceFile, @transient sc: SparkContext) {


  /**
   * Extracts sequence from a referenceRegion
   * @param points
   */
  def extractSequences(points: RDD[ReferenceRegion]): RDD[(ReferenceRegion, String)] = {
    // used to get middle of peak sized at the sequence size
    val broadcast2Bit = points.context.broadcast(reference)
    points.map(r => {
      (r, broadcast2Bit.value.extract(r).toUpperCase())
    })
  }
}

object Sequence {

  def apply(referencePath: String, sc: SparkContext): Sequence = {
    // Load reference Path
    val reference: ReferenceFile =
      if (referencePath.endsWith(".fa") || referencePath.endsWith(".fasta"))
        sc.loadReferenceFile(referencePath, 10000)
      else if (referencePath.endsWith(".2bit"))
        if (sc.isLocal) {
          val ref = new File(referencePath)
          new TwoBitFile(new LocalFileByteAccess(ref))
        } else {
          val file = new File(referencePath)
          var stream: FileInputStream = null
          val bytes: Array[Byte] = Array.fill[Byte](file.length().toInt)(0)
          try {
            stream = new FileInputStream(file)
            stream.read(bytes)
            stream.close()
          } catch {
            case e: Exception => println(e.fillInStackTrace())
          }
          new TwoBitFile(new ByteArrayByteAccess(bytes))
        }
      else throw new IllegalArgumentException("Unsupported reference file format")
    new Sequence(reference, sc)
  }

  // for testing purposes
  def apply(reference: RDD[NucleotideContigFragment], sc: SparkContext): Sequence = {
    new Sequence(ReferenceContigMap(reference), sc)
  }
}

case class SequenceSet[S: ClassTag, T: ClassTag](features: RDD[S], labels: RDD[T])
