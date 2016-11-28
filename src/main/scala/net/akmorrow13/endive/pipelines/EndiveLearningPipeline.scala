package net.akmorrow13.endive.pipelines

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import net.akmorrow13.endive.EndiveConf
import nodes.learning.BlockLinearMapper
import org.apache.spark.rdd.RDD

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
      val inp = new ObjectInputStream(new FileInputStream("/Users/DevinPetersohn/Downloads/testSerializedModelOutput"))
      val predictor = inp.readObject().asInstanceOf[BlockLinearMapper]
      inp.close()
      Some(predictor)
    } catch {
      case e: Exception => None
    }
  }

  def apply(filepath: String): RDD[Double]

}
