package net.akmorrow13.endive.processing

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.{ReferencePosition, SequenceDictionary, ReferenceRegion}
import org.bdgenomics.formats.avro.Strand
import net.akmorrow13.endive.utils.{LabeledWindow, Window }

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

}

class Dnase(@transient windowSize: Int,
                       @transient stride: Int,
                        @transient sc: SparkContext,
                       dnaseCuts: RDD[Cut]) extends Serializable {

  /**
   * Merges Cuts into an rdd that maps each point in the genome to a map of cuts, where the map specifies the cell type
   * If the cell Type has no cuts in a region, that cell type is excluded from the map
   * @param sd
   * @return
   */
  def merge(sd: SequenceDictionary): RDD[CutMap] = {
    val chrs = dnaseCuts.map(_.region.referenceName).distinct().collect
    val reducedRecords = sc.parallelize(sd.records.filter(r => chrs.contains(r.name)), chrs.length)  // filter out data in dnase

    processCuts()
        .groupBy(r => r.position)
        .map(r => {
          CutMap(r._1, r._2.map(r => (r.cellType, r.count)).toMap)
        })
  }

  /**
   * reduces all cuts processed from AlignmentRecords to all cuts aggregated at a base pair
   * granularity. This function treats negative and positives strands the same. Note that
   * CutLoader accounts for base shifting for dnase datasets.
   * @return Aggregated cuts summing counts at every region
   */
  def processCuts(): RDD[AggregatedCut] = {
    val counts:RDD[((CellTypes.Value, ReferencePosition), Int)] =
      dnaseCuts
        .flatMap(r => {
          val startCut = ReferencePosition(r.region.referenceName, r.region.start, if (r.negativeStrand) Strand.REVERSE else Strand.FORWARD)
          val endCut = ReferencePosition(r.region.referenceName, r.region.end, if (r.negativeStrand) Strand.REVERSE else Strand.FORWARD)
          Iterable(((r.getCellType, startCut), 1), ((r.getCellType, endCut), 1))
        })

    counts.reduceByKey(_ + _).map(r => AggregatedCut(r._1._1, r._1._2, r._2))
  }
}

case class Cut(region: ReferenceRegion, experimentId: String, readId: String, negativeStrand: Boolean) {
  override
  def toString: String = {
    s"${region.referenceName}${Window.STDDELIM}${region.start}${Window.STDDELIM}${region.end}${Window.STDDELIM}${experimentId}${Window.STDDELIM}${readId}${Window.STDDELIM}${negativeStrand}"
  }

  def getCellType: CellTypes.Value = {
    CellTypes.withName(experimentId.split(Window.STDDELIM)(1))
  }
}

case class AggregatedCut(cellType: CellTypes.Value, position: ReferencePosition, count: Int) {
}

object CutLoader {

  def stringToCut(str: String): Cut = {
    val data = str.split(Window.STDDELIM)
    val negativeStrand = data(5).toBoolean
    val region =
      if (negativeStrand)
        ReferenceRegion(data(0), data(1).toLong-1, data(2).toLong-1) // subtract 1 from negative strand (bio)
      else
        ReferenceRegion(data(0), data(1).toLong, data(2).toLong)

    Cut(region, data(3), data(4), negativeStrand)
  }

  def apply(path: String, sc: SparkContext): RDD[Cut] = {
    val dataTxtRDD:RDD[String] = sc.textFile(path)
    dataTxtRDD.map(stringToCut(_))
  }
}
case class DnaseWindow(region: ReferenceRegion, counts: Array[Int])


/**
 * For a given position maps all cell types that have data at that region
 * @param position 1 bp position in the genome
 * @param countMap map of all celltypes and their corresponding counts at that siteG
 */
case class CutMap(position: ReferencePosition, countMap: Map[CellTypes.Value, Int])
