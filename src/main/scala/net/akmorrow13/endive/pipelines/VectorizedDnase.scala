/**
 * Copyright 2015 Frank Austin Nothaft
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.akmorrow13.endive.pipelines

import scala.reflect.ClassTag
import breeze.linalg.DenseVector
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.Motif
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import nodes.learning.LogisticRegressionEstimator
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{Coverage, ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.rdd.feature.CoverageRDD
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.{Strand, AlignmentRecord, Feature}
import org.bdgenomics.adam.rdd.LeftOuterShuffleRegionJoin
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import org.bdgenomics.adam.rdd.ADAMContext._


object VectorizedDnase extends Serializable  {

  /**
   * A very basic dataset creation pipeline that *doesn't* featurize the data
   * but creates a csv of (Window, Label)
   *
    * @param args
   */
  def main(args: Array[String]) = {

    if (args.size < 1) {
      println("Incorrect number of arguments...Exiting now.")
    } else {
      val configfile = scala.io.Source.fromFile(args(0))
      val configtext = try configfile.mkString finally configfile.close()
      println(configtext)
      val yaml = new Yaml(new Constructor(classOf[EndiveConf]))
      val appConfig = yaml.load(configtext).asInstanceOf[EndiveConf]
      EndiveConf.validate(appConfig)
      val conf = new SparkConf().setAppName("ENDIVE")
      conf.setIfMissing("spark.master" ,  "local[4]" )
      val sc = new SparkContext(conf)
      run(sc, appConfig)
      sc.stop()
    }
  }

  def run(sc: SparkContext, conf: EndiveConf) {
    println("STARTING VECTORIZED PIPELINE")

    // challenge parameters
    val windowSize = 200
    val stride = 50

    // create new sequence with reference path
    val referencePath = conf.reference
    // load chip seq labels from 1 file
    val labelsPath = conf.labels
    val dnasePath = conf.dnaseBams

    // load in 1 transcription factor
    /*
      val windowsRDD: RDD[LabeledWindow] = Preprocess.loadLabels(sc, labelsPath)
      ._1.map(r => LabeledWindow(Window(r._1, r._2, r._3, "N" * 200), r._4))
      .cache()
    */
    val windowsRDD: RDD[LabeledWindow] = sc.textFile(labelsPath).map(s => LabeledWindowLoader.stringToLabeledWindow(s))
    val sd = DatasetCreationPipeline.getSequenceDictionary(referencePath)

    /* First one chromesome and one celltype per fold (leave 1 out) */
    val folds = EndiveUtils.generateFoldsRDD(windowsRDD.keyBy(r => (r.win.getRegion.referenceName, r.win.getCellType)), conf.heldOutCells, conf.heldoutChr, conf.folds)
    val chrCellTypes: Iterable[(String, CellTypes.Value)] = windowsRDD.map(x => (x.win.getRegion.referenceName, x.win.getCellType)).countByValue().keys
    val cellTypes = chrCellTypes.map(_._2)

    println("Cell types: ")
    cellTypes.toArray.foreach(println)
    
    // load dnase file (contains all cell types)
    val dnase = sc.loadAlignments(dnasePath)


    println("TOTAL FOLDS " + folds.size)
    for (i <- (0 until folds.size)) {
      println("FOLD " + i)
      // get testing cell types for this fold
      val cellTypesTest = folds(i)._2.map(x => (x._2.win.getCellType)).countByValue().keys.toList
      println(s"Fold ${i}, testing cell types:")
      cellTypesTest.foreach(println)

      // get testing chrs for this fold
      val chromosomesTest:Iterable[String] = folds(i)._2.map(x => (x._2.win.getRegion.referenceName)).countByValue().keys
      println(s"Fold ${i}, testing chromsomes:")
      chromosomesTest.foreach(println)

      // calculate features for train and test sets
      val train = featurize(sc, folds(i)._1.map(_._2), dnase, sd, subselectNegatives = true).cache()
      val test = featurize(sc, folds(i)._2.map(_._2), dnase, sd, subselectNegatives = false).cache()

      println("TRAIN SIZE IS " + train.count())
      println("TEST SIZE IS " + test.count())

      // training features
      val xTrainPositives = train.map(_.win.getDnase).filter(r => r.findAll(_ > 0).size > 0)
        .setName("XTrainPositives").cache()
      val xTrainNegatives = train.map(_.win.getDnase).filter(r => r.findAll(_ > 0).size == 0)
        .setName("XTrainNegatives").cache()
      println("training positives and negatives", xTrainPositives.count, xTrainNegatives.count)

      // testing features
      val xTestPositives: RDD[DenseVector[Double]] = test.map(_.win.getDnase).filter(r => r.findAll(_ > 0).size > 0)
  .setName("XTestPositives").cache()
      val xTestNegatives: RDD[DenseVector[Double]] = test.map(_.win.getDnase).filter(r => r.findAll(_ > 0).size == 0)
  .setName("XTestNegatives").cache()
      println("testing positives and negatives", xTestPositives.count, xTestNegatives.count)
 
      // training labels
      val yTrainPositives = train.filter(r => r.win.getDnase.findAll(_ > 0).size > 0).map(_.label)
        .setName("yTrainPositive").cache()
      val yTrainNegatives = train.filter(r => r.win.getDnase.findAll(_ > 0).size == 0).map(_.label)
        .setName("yTrainNegative").cache()
      val yTrain = yTrainPositives.union(yTrainNegatives).map(_.toDouble)
      
      // testing labels

      val yTestPositives = test.filter(r => r.win.getDnase.findAll(_ > 0).size > 0).map(_.label)
        .setName("yTestPositive").cache()
      val yTestNegatives = test.filter(r => r.win.getDnase.findAll(_ > 0).size == 0).map(_.label)
        .setName("yTestNegative").cache()
      val yTest = yTestPositives.union(yTestNegatives).map(_.toDouble)

      println("Training model")
      val predictor = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 10).fit(xTrainPositives, yTrainPositives)

      val yPredTrain = predictor(xTrainPositives).union(xTrainNegatives.map(r => 0.0))
      val evalTrain = new BinaryClassificationMetrics(yPredTrain.zip(yTrain.map(_.toDouble)))
      println("Train Results: \n ")
      Metrics.printMetrics(evalTrain)

      val yPredTest = predictor(xTestPositives).union(xTestNegatives.map(r => 0.0))
      val evalTest = new BinaryClassificationMetrics(yPredTest.zip(yTest.map(_.toDouble)))
      println("Test Results: \n ")
      Metrics.printMetrics(evalTest)

    }
  }

  /**
   * Joins Coverage and windows. does not specify cell type
   */
  def joinWithDnaseCoverage(sc: SparkContext,
                        sd: SequenceDictionary,
                        filteredRDD: RDD[LabeledWindow],
                        positiveCoverage: CoverageRDD,
                        negativeCoverage: CoverageRDD): RDD[LabeledWindow] = {


    // cache all rdds for join
    filteredRDD.setName("filteredRDD").cache()
    println("rdd count", filteredRDD.count)


    // joining is not cell type specific. require only one cell type
    require(filteredRDD.map(_.win.getCellType).distinct.count() == 1, "join does not specifiy cell type. " +
      "must only have one cell types")


    // keep track of strands
    val positiveFeatures = positiveCoverage.toFeatureRDD.rdd.map(r => {
      r.setStrand(Strand.FORWARD)
      r
    })

    val negativeFeatures = negativeCoverage.toFeatureRDD.rdd.map(r => {
      r.setStrand(Strand.REVERSE)
      r
    })

    // join together coverage
    val coverageFeatures = positiveFeatures.union(negativeFeatures)

    coverageFeatures.rdd.setName("positiveFeatures").cache()
    println(s"coverage count in vectorized dnase ${coverageFeatures.rdd.count}")

    // join with positive strands
    var joinedRDD =
      LeftOuterShuffleRegionJoin[LabeledWindow, Feature](sd, 1000000, sc)
        .partitionAndJoin(filteredRDD.keyBy(_.win.getRegion), coverageFeatures.keyBy(r => ReferenceRegion(r.contigName, r.start, r.end)))

     sc.setCheckpointDir("/data/anv/DREADMDATA/checkpoint/")
     joinedRDD = truncateLineage(joinedRDD, true).setName("joinedRDD")

     coverageFeatures.rdd.unpersist()

     val cutsAndWindows = joinedRDD
        .groupBy(r => (r._1.win.getRegion, r._1.win.getTf))
        .map(r => {
          val arr = r._2.toArray
          featurizeCoverage(arr.head._1, arr.map(_._2).filter(_.isDefined).map(_.get))
        }).setName("cutsAndWindows")
        .cache()

    filteredRDD.unpersist()
    println("Cuts and Windows Count: " + cutsAndWindows.count)

    cutsAndWindows

  }

  def joinWithDnaseBams(sc: SparkContext,
                        sd: SequenceDictionary,
                        filteredRDD: RDD[LabeledWindow],
                        coverage: AlignmentRecordRDD,
                        partitionCount: Int = 2000000): RDD[LabeledWindow] = {

    // this is a hack to force LeftOuterShuffleRegionJoin work.
    // We must force all reads to be mapped
    val mappedCoverage = coverage.transform(r => r.map(ar => {
      ar.setReadMapped(true)
      ar
    }).filter(_.start >= 0 ))

    filteredRDD.setName("filteredRDD").cache()
    println("rdd count", filteredRDD.count)
    mappedCoverage.rdd.setName("mappedCoverage").cache()
    mappedCoverage.rdd.count

    val cutsAndWindows: RDD[(LabeledWindow, Option[AlignmentRecord])] =
      LeftOuterShuffleRegionJoin[LabeledWindow, AlignmentRecord](sd, partitionCount, sc)
        .partitionAndJoin(filteredRDD.keyBy(_.win.getRegion), mappedCoverage.rdd.keyBy(r => ReferenceRegion.stranded(r)))
        .filter(_._2.isDefined)
        .setName("cutsAndWindows")
        .cache()

    println(s"Final partition count: ${cutsAndWindows.partitions.length}, count: ${cutsAndWindows.count}")

    mappedCoverage.rdd.unpersist()
    filteredRDD.unpersist()

    val groupedWindows: RDD[(LabeledWindow, Iterable[AlignmentRecord])] = cutsAndWindows
          .groupBy(r => (r._1.win.getRegion, r._1.win.getCellType, r._1.win.getTf))
          .map(r => {
            val x = r._2.toList
            (x.head._1, x.map(_._2.get).toIterable)
      })
      .setName("groupedWindows")
      .cache()

    println("Cuts and Windows Count: " + groupedWindows.count)
    cutsAndWindows.unpersist()

    // featurize datapoints to vector of numbers
    featurizePositives(groupedWindows, false)

  }

  /**
   * Featurize with full calcuated dnase coverage
    *
    * @param sc SparkContext
   * @param rdd RDD of labeledWindow
   * @param coverage point coverage
   * @param sd SequenceDictionary
   * @param subselectNegatives whether or not to subselect the dataset
   * @param filterRegionsByDnasePeaks: if true, does not featurize raw dnase with now peaks and thresholds these
   *      features to 0
   * @return
   */
  def featurize(sc: SparkContext,
                 rdd: RDD[LabeledWindow],
                 coverage: AlignmentRecordRDD,
                 sd: SequenceDictionary,
                 subselectNegatives: Boolean = true,
                 filterRegionsByDnasePeaks: Boolean = false,
                 motifs: Option[List[Motif]] = None,
                 doCentipede: Boolean = true): RDD[LabeledWindow] = {


    // perform optional negative sampling
    var filteredRDD =
      if (subselectNegatives)
        EndiveUtils.subselectSamples(sc, rdd, sd, partition = false)
      else
        rdd

    joinWithDnaseBams(sc, sd, filteredRDD, coverage)

  }

  /**
   * Featurizes cuts from dnase to vectors of numbers covering 200 bp windows
   * @param window cuts and windows to vectorize
   * @return
   */
  def featurizeCoverage(window: LabeledWindow,
                        coverage: Array[Feature]): LabeledWindow = {

    val cellType = window.win.getCellType

    // where to center motif?
    val (start, end) =
        (window.win.getRegion.start, window.win.getRegion.end)

    val positiveCoverage = coverage.filter(r => r.getStrand == Strand.FORWARD)
    val negativeCoverage = coverage.filter(r => r.getStrand == Strand.REVERSE)


    val windowSize = window.win.getRegion.length().toInt
    println(s"windowSize: ${windowSize}")

    val positivePositions = DenseVector.zeros[Double](windowSize)
    val negativePositions = DenseVector.zeros[Double](windowSize)

    // featurize positives to vector
    positiveCoverage.foreach(r => {
      positivePositions((r.start - start).toInt) = r.getScore
    })

    // featurize negatives to vector
    negativeCoverage.foreach(r => {
      negativePositions((r.start - start).toInt) = r.getScore
    })

    val positions = DenseVector.vertcat(positivePositions, negativePositions)

    // positions should be size of window
    LabeledWindow(window.win.setDnase(positions), window.label)
  }


  /**
   * Featurizes cuts from dnase to vectors of numbers covering 200 bp windows
   * @param cutsAndWindows cuts and windows to vectorize
   * @param doCentipede Whether or not to concolve using msCentipede algorithm
   * @return
   */
  def featurizePositives(cutsAndWindows: RDD[(LabeledWindow, Iterable[AlignmentRecord])],
                doCentipede: Boolean = true): RDD[LabeledWindow] = {

    // filter windows into regions with and without relaxed dnase peaks

    val windowSize = cutsAndWindows.first._1.win.getRegion.length().toInt
    println(s"windowSize: ${windowSize}")

    val centipedeWindows: RDD[LabeledWindow] =
      cutsAndWindows.map(window => {
        val cellType = window._1.win.getCellType

        // where to center motif?
        val (start, end) =
            (window._1.win.getRegion.start, window._1.win.getRegion.end)

        val positivePositions = DenseVector.zeros[Int](windowSize)
        val negativePositions = DenseVector.zeros[Int](windowSize)

        window._2.toList.foreach(rec => {
          if (rec.getReadNegativeStrand)
            negativePositions((rec.getStart - start).toInt) += 1
          else
            positivePositions((rec.getStart - start).toInt) += 1
        })

        val positions =
          if (doCentipede)
            DenseVector.vertcat(Dnase.msCentipede(positivePositions)
              , Dnase.msCentipede(negativePositions))
          else DenseVector.vertcat(positivePositions
              , negativePositions).map(_.toDouble)

        // positions should be size of window
        LabeledWindow(window._1.win.setDnase(positions), window._1.label)
      })
    centipedeWindows
  }
 
 def truncateLineage[T: ClassTag](in: RDD[T], cache: Boolean): RDD[T] = {
    // What we are doing here is:
    // cache the input before checkpoint as it triggers a job
    if (cache) {
      in.cache()
    }
    in.checkpoint()
    // Run a count to trigger the checkpoint
    in.count

    // Now "in" has HDFS preferred locations which is bothersome
    // when we zip it next time. So do a identity map & get an RDD
    // that is in memory, but has no preferred locs
    val out = in.map(x => x).cache()
    // This stage will run as NODE_LOCAL ?
    out.count

    // Now out is in memory, we can get rid of "in" and then
    // return out
    if (cache) {
      in.unpersist(true)
    }
    out
  }
}
