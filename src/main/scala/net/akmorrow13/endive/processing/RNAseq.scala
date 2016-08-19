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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion

class RNAseq(geneReference: String, @transient sc: SparkContext) {

  val genes: RDD[Transcript] = Preprocess.loadTranscripts(sc, geneReference)


  def loadRNA(sc: SparkContext, filePath: String): RDD[(String, RNARecord)] = {
    val cellType = filePath.split("/").last.split('.')(1)
    val genesB = sc.broadcast(genes.collect.toList)

    val data = Preprocess.loadTsv(sc, filePath, "gene_id")
    val records = data.flatMap(parts => {
      // parse text file
      val (geneId, length, effective_length, expected_count, tpm,	fpkm)
        = (parts(0), parts(2).toDouble, parts(3).toDouble, parts(4).toDouble, parts(5).toDouble, parts(6).toDouble)

      val filteredTranscripts: List[Transcript] = genesB.value.filter(p => p.geneId == geneId) // filter out relevent genes

      filteredTranscripts.map(t => (RNARecord(t.region, geneId, length, effective_length, expected_count, tpm, fpkm)))
    })
    records.map(r => (cellType, r))
  }

  def loadRNAFolder(sc: SparkContext, folder: String): RDD[(String, RNARecord)] = {
    var data: RDD[(String, RNARecord)] = sc.emptyRDD[(String, RNARecord)]
    if (sc.isLocal) {
      val d = new File(folder)
      if (d.exists && d.isDirectory) {
        val files = d.listFiles.filter(_.isFile).toList
        files.map(f => {
          data = data.union(loadRNA(sc, f.getPath))
        })
      } else {
        throw new Exception(s"${folder} is not a valid directory for peaks")
      }
    } else {
      try{
        val fs: FileSystem = FileSystem.get(new Configuration())
        val status = fs.listStatus(new Path(folder))
        for (i <- status) {
          val file: String = i.getPath.getName
          data = data.union(loadRNA(sc, file))
        }
      } catch {
        case e: Exception => println(s"Directory ${folder} could not be loaded")
      }
    }
    data
  }
}

