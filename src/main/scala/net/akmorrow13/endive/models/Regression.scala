
package net.akmorrow13.models

import org.apache.spark.rdd.RDD
import org.bdgenomics.formats.avro.{Feature, NucleotideContigFragment}

object SequenceRegression {
  def apply(data: RDD[Feature], sequences: RDD[NucleotideContigFragment]) = {

//    val parsedData = data.map { line =>
//      val parts = line.split(',')
//      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
//    }.cache()
//
//    // Build the model
//    val numIterations = 100
//    val stepSize = 0.00000001
//    val regParam = 1
//    val model = LassoWithSGD.train(parsedData, numIterations, stepSize, regParam)
//
//    // Evaluate model on training examples and compute training error
//    val valuesAndPreds = parsedData.map { point =>
//      val prediction = model.predict(point.features)
//      (point.label, prediction)
//    }
//    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
//    println("training Mean Squared Error = " + MSE)


  }
}