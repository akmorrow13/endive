package net.akmorrow13.endive.pipelines

import net.akmorrow13.endive.processing.{CellTypes, TranscriptionFactors, Preprocess}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.models.ReferenceRegion

/**
  * Created by DevinPetersohn on 8/22/16.
  *
**/

object BoundCorrTest {

  def prepareTest(sc: SparkContext, filepath: String): RDD[(TranscriptionFactors.Value, CellTypes.Value, ReferenceRegion, Int)] = {
    Preprocess.loadLabelFolder(sc, filepath)
  }
}