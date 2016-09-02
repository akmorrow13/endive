package net.akmorrow13.endive.utils

import breeze.numerics.{round, ceil}
import org.apache.spark.Partitioner
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import org.bdgenomics.utils.misc.Logging


case class GenomicRegionPartitioner(partitionSize: Long, seqLengths: Map[String, Long], start: Boolean = true) extends Partitioner with Logging {
  private val names: Seq[String] = seqLengths.keys.toSeq.sortWith(_ < _)
  private val lengths: Seq[Long] = names.map(seqLengths(_))
  private val parts: Seq[Int] = lengths.map(v => round(ceil(v.toDouble / partitionSize)).toInt)
  private val cumulParts: Map[String, Int] = Map(names.zip(parts.scan(0)(_ + _)): _*)

  private def computePartition(refReg: ReferenceRegion): Int = {
    val pos = if (start) refReg.start else (refReg.end - 1)
    (cumulParts(refReg.referenceName) + pos / partitionSize).toInt
  }

  override def numPartitions: Int = parts.sum

  override def getPartition(key: Any): Int = {
    key match {
      case region: ReferenceRegion => {
        require(
          seqLengths.contains(region.referenceName),
          "Received key (%s) that did not map to a known contig. Contigs are:\n%s".format(
            region,
            seqLengths.keys.mkString("\n")
          )
        )
        computePartition(region)
      }
      case (region: ReferenceRegion, k: Any) => {
        require(
          seqLengths.contains(region.referenceName),
          "Received key (%s) that did not map to a known contig. Contigs are:\n%s".format(
            region,
            seqLengths.keys.mkString("\n")
          )
        )
        computePartition(region)
      }
      case _ => throw new IllegalArgumentException("Only ReferenceMappable values can be partitioned by GenomicRegionPartitioner")
    }
  }
}

object GenomicRegionPartitioner {

  /**
   * Creates a GenomicRegionPartitioner where partitions cover a specific range of the genome.
   *
   * @param partitionSize The number of bases in the reference genome that each partition should cover.
   * @param seqDict A sequence dictionary describing the known genomic contigs.
   * @return Returns a partitioner that divides the known genome into partitions of fixed size.
   */
  def apply(partitionSize: Long, seqDict: SequenceDictionary): GenomicRegionPartitioner =
    GenomicRegionPartitioner(partitionSize, extractLengthMap(seqDict))

  /**
   * Creates a GenomicRegionPartitioner with a specific number of partitions.
   *
   * @param numParts The number of partitions to have in the new partitioner.
   * @param seqDict A sequence dictionary describing the known genomic contigs.
   * @return Returns a partitioner that divides the known genome into a set number of partitions.
   */
  def apply(numParts: Int, seqDict: SequenceDictionary): GenomicRegionPartitioner = {
    val lengths = extractLengthMap(seqDict)
    GenomicRegionPartitioner(lengths.values.sum / numParts, lengths)
  }

  def extractLengthMap(seqDict: SequenceDictionary): Map[String, Long] =
    seqDict.records.toSeq.map(rec => (rec.name, rec.length)).toMap
}
