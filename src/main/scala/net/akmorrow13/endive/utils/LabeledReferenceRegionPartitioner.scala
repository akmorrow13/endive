package net.akmorrow13.endive.utils

import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.{ReferenceRegion, ReferencePosition, SequenceDictionary}

/**
 * Repartitions objects that are keyed by a ReferencePosition or ReferenceRegion
 * into a single partition per contig.
 */
case class LabeledReferenceRegionPartitioner(sd: SequenceDictionary) extends Partitioner {

  // extract just the reference names
  private val referenceNames = sd.records.map(_.name)

  override def numPartitions: Int = referenceNames.length

  private def partitionFromName(name: String): Int = {
    // which reference is this in?
    val pIdx = referenceNames.indexOf(name)

    // provide debug info to user if key is bad
    assert(pIdx != -1, "Reference not found in " + sd + " for key " + name)

    pIdx
  }

  override def getPartition(key: Any): Int = key match {
    case rp: (ReferenceRegion, String) => {
      partitionFromName(rp._1.referenceName)
    }
    case _ => throw new IllegalArgumentException("Only labeled ReferenceRegions can be used as a key.")
  }
}