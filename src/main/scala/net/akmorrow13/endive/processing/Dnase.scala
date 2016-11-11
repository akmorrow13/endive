package net.akmorrow13.endive.processing

import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ ReferencePosition }
import org.bdgenomics.formats.avro.{ AlignmentRecord }

object Dnase {

  val lnOf2 = scala.math.log(2) // natural log of 2
  def log2(x: Double): Int = Math.floor(scala.math.log(x) / lnOf2).toInt

  def msCentipede(r: Array[Int], scale: Option[Int] = None): Array[Double] = {
    val j =
      if (scale.isDefined) scale.get + 1
      else log2(r.length) + 1

      // iterate through all scales s
      (0 until j).flatMap(s => {
        // create ith scale for parameter vector (see mscentipede to calculate model at bound motifs
        if (s == 0) Array(r.sum.toDouble)
        else {
          val numeratorLength = Math.round(r.length/(Math.pow(2,s))).toInt
          val denominatorLength =  Math.round(r.length/(Math.pow(2,s)) * 2).toInt
          val x: Array[Double] = (0 until Math.pow(2, s-1).toInt).map(i => {
            val numeratorSum = r.slice(i * denominatorLength, i * denominatorLength + numeratorLength).sum
            val denominator = r.slice(i * denominatorLength, i * denominatorLength + denominatorLength).sum
            val denominatorSum = if (denominator == 0) 1 else denominator
            numeratorSum.toDouble/denominatorSum
          }).toArray
          x
        }
      }).toArray
  }

  def centipedeRDD(windows: RDD[Array[Int]], scale: Option[Int] = None): RDD[Array[Double]] = {
    windows.map(r => msCentipede(r, scale))
  }

  /**
   * Generates cuts from an AlignmentRecord by parsing the start and end of each AlignmentRecord.
   * Shifts negative strands back by 1 to account for protein specific alignment
   * @param ar AlignmentRecord to parse
   * @return Tuple of start and end alignment fragments where DNA was cut
   */
  def generateCuts(ar: AlignmentRecord, cellType: String, id: String): Iterator[AlignmentRecord] = {
    // generate ar for start and end of fragment
    val startAr = ar
    val endAr = AlignmentRecord.newBuilder(ar).build()

    // if negative strand shift everything back by one base
    if (ar.getReadNegativeStrand) {
      // end of fragment
      endAr.setStart(ar.getEnd - 1)
      endAr.setEnd(endAr.getStart + 1)

      // start of fragment
      startAr.setStart(ar.getStart - 1)
      startAr.setEnd(startAr.getStart + 1)
    } else {
      // end of fragment
      endAr.setStart(ar.getEnd)
      endAr.setEnd(endAr.getStart + 1)

      // start of fragment
      startAr.setEnd(startAr.getStart + 1)
    }

    // set cell type
    val attr = cellType + ":" + id
    startAr.setAttributes(attr)
    endAr.setAttributes(attr)
    Iterator(startAr, endAr)
  }

}
