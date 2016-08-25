package net.akmorrow13.endive.processing

import breeze.linalg.DenseVector
import net.akmorrow13.endive.EndiveFunSuite
import java.util._
import nodes.learning.LogisticRegressionEstimator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext


class PreprocessSuite extends EndiveFunSuite {

  // training data of region and labels
  var peakPath = resourcePath("DNASE.A549.conservative.head30.narrowPeak")

  val tsvPath = resourcePath("labels/EGR1.train.labels.tsv.gz")

  /*
  sparkTest("should extract peak records from narrowPeak file") {

    val trainRDD = Preprocess.loadPeaks(sc, peakPath)
    val rec = trainRDD.first
    assert(trainRDD.count == 30)
    assert(rec._2.score == 866)
    assert(rec._2.signalValue == 1.31833)
    assert(rec._2.pValue == 2.19291)
    assert(rec._2.qValue == 0.61886)
    assert(rec._2.peak == 724)
  }
  */

  sparkTest("should extract labels from tsv files"){
    val tsvRDD = Preprocess.loadLabelFolder(sc, resourcePath("labels"))
    //tsvRDD.filter(_._2 == "GM12878").take(20).foreach(println)
    val data = tsvRDD.filter(_._2 == "GM12878").map(f => (f._3,(f._1,f._4))).groupByKey.sortByKey(ascending = true).map(f => f._2.toArray.sortBy(_._1).map(g => g._2))
    var countMap = scala.collection.mutable.Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0, 5 -> 0, 6 -> 0)
    for(item <- data.map(f => f.filter(_ != -1).sum).collect){
      countMap(item) += 1
    }
    countMap.foreach(println)
    /*
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val training = data.sample(withReplacement = true, 0.5, 1337).toDF()

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    */

    /*
    val y = data.zipWithIndex.map(f => (f._2.toInt,f._1)).keys
    val predictor = LogisticRegressionEstimator[DenseVector[(String,Int)]](numClasses = 2,numIters = 1).fit(data,y)
    val evalTest = new BinaryClassificationEvaluator(predictor(data))
    */
  }
}
