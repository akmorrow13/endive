package net.akmorrow13.endive.utils

import java.util

import net.akmorrow13.endive.processing.CellTypes
import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.{ReferenceRegion, ReferencePosition, SequenceDictionary}

/**
 * Repartitions objects that are keyed by a ReferencePosition or ReferenceRegion
 * into a single partition per contig.
 */
case class LabeledReferenceRegionPartitioner(sd: SequenceDictionary, indexer: Enumeration = CellTypes) extends Partitioner {

  // extract all combinations of chromosome, celltype
  private val reference: Vector[(String, indexer.Value)] = for (x <- sd.records.map(_.name); y <- indexer.values) yield (x,y)

  override def numPartitions: Int = reference.length

  private def partitionFromName(referenceName: String, idx: AnyRef): Int = {
    // which reference is this in?
    val pIdx = reference.indexOf((referenceName, idx))

    // provide debug info to user if key is bad
    assert(pIdx != -1, "Reference not found in " + reference + " for key " + referenceName + " , " + idx)

    pIdx
  }

  override def getPartition(key: Any): Int = key match {
    case rp: (ReferenceRegion, AnyRef) => {
      partitionFromName(rp._1.referenceName, rp._2)
    }
    case _ => throw new IllegalArgumentException("Only labeled ReferenceRegions can be used as a key.")
  }
}