package net.akmorrow13.endive.pipelines

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import breeze.linalg.DenseMatrix
import breeze.stats.distributions.Gaussian
import nodes.learning.BlockLinearMapper
import nodes.util.MaxClassifier
import org.bdgenomics.adam.models.ReferenceRegion
import org.bdgenomics.formats.avro.Feature

/**
  * Created by DevinPetersohn.
  */
abstract class EndiveLearningPipeline {

  def saveModel(filepath: String, model: BlockLinearMapper) = {
    try {
      val writer = new ObjectOutputStream(new FileOutputStream(filepath))
      writer.writeObject(model)
      writer.close()
    }
  }

  def loadModel(filepath: String): Option[BlockLinearMapper] = {
    try {
      val inp = new ObjectInputStream(new FileInputStream(filepath))
      val predictor = inp.readObject().asInstanceOf[BlockLinearMapper]
      inp.close()
      Some(predictor)
    } catch {
      case e: Exception => None
    }
  }

}

/**
 * Serves sequence model. filePath should be estimator on sequence data, predicting DenseVector of classes
 * @param filePath file path to model
 */
case class ModelServer(filePath: String, dim: Int = 4096) {

  // predictor
  val inp = new ObjectInputStream(new FileInputStream(filePath))
  val predictor = inp.readObject().asInstanceOf[BlockLinearMapper]
  inp.close()

  def name = filePath
  // generate random matrix
  val gaussian = new Gaussian(0, 1)
  val W = DenseMatrix.rand(dim, 8*4, gaussian)

  def serve(in: (ReferenceRegion, String)): Array[Feature] = {
    val region = in._1
    val testSequences = in._2.sliding(200, 50).toArray

    val regions = Array.range(0, in._1.length().toInt)
      .sliding(200, 50)
      .map(r => ReferenceRegion(region.referenceName, r.head + region.start, r.head + region.start + r.length))
      .toArray

    // featurization step
    val trainApprox = KernelPipeline.featurizeStrings(testSequences, W, 8)

    trainApprox.map(r => MaxClassifier(predictor(r))).zip(regions)
      .map(r => {
      Feature.newBuilder()
        .setContigName(r._2.referenceName)
        .setStart(r._2.start)
        .setEnd(r._2.end)
        .setScore(r._1.toDouble)
        .build()
    })
  }
}
