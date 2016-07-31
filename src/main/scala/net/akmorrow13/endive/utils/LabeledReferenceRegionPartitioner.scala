package net.akmorrow13.endive.utils

import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.{ReferenceRegion, ReferencePosition, SequenceDictionary}

/**
 * Repartitions objects that are keyed by a ReferencePosition or ReferenceRegion
 * into a single partition per contig.
 */
case class LabeledReferenceRegionPartitioner(sd: SequenceDictionary, cellTypes: Vector[String]) extends Partitioner {

  // extract all combinations of chromosome, celltype
  private val reference: Vector[(String, String)] = for (x <- sd.records.map(_.name); y <- cellTypes) yield (x,y)

  override def numPartitions: Int = reference.length

  private def partitionFromName(referenceName: String, cellType: String): Int = {
    // which reference is this in?
    val pIdx = reference.indexOf((referenceName, cellType))

    // provide debug info to user if key is bad
    assert(pIdx != -1, "Reference not found in " + reference + " for key " + referenceName + " , " + cellType)

    pIdx
  }

  override def getPartition(key: Any): Int = key match {
    case rp: (ReferenceRegion, String) => {
      partitionFromName(rp._1.referenceName, rp._2)
    }
    case _ => throw new IllegalArgumentException("Only labeled ReferenceRegions can be used as a key.")
  }
}