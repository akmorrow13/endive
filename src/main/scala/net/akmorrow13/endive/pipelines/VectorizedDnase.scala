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
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD
import org.bdgenomics.formats.avro.AlignmentRecord
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
    val folds = EndiveUtils.generateFoldsRDD(windowsRDD.keyBy(r => (r.win.region.referenceName, r.win.cellType)), conf.heldOutCells, conf.heldoutChr, conf.folds)
    val chrCellTypes: Iterable[(String, CellTypes.Value)] = windowsRDD.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys
    val cellTypes = chrCellTypes.map(_._2)

    println("Cell types: ")
    cellTypes.toArray.foreach(println)
    
    // load dnase file (contains all cell types)
    val dnase = sc.loadAlignments(dnasePath)


    println("TOTAL FOLDS " + folds.size)
    for (i <- (0 until folds.size)) {
      println("FOLD " + i)
      // get testing cell types for this fold
      val cellTypesTest = folds(i)._2.map(x => (x._2.win.cellType)).countByValue().keys.toList
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
      val xTrainPositives = train.map(_.win.dnase).filter(r => r.findAll(_ > 0).size > 0)
        .setName("XTrainPositives").cache()
      val xTrainNegatives = train.map(_.win.dnase).filter(r => r.findAll(_ > 0).size == 0)
        .setName("XTrainNegatives").cache()
      println("training positives and negatives", xTrainPositives.count, xTrainNegatives.count)

      // testing features
      val xTestPositives: RDD[DenseVector[Double]] = test.map(_.win.dnase).filter(r => r.findAll(_ > 0).size > 0)
  .setName("XTestPositives").cache()
      val xTestNegatives: RDD[DenseVector[Double]] = test.map(_.win.dnase).filter(r => r.findAll(_ > 0).size == 0)
  .setName("XTestNegatives").cache()
      println("testing positives and negatives", xTestPositives.count, xTestNegatives.count)
 
      // training labels
      val yTrainPositives = train.filter(r => r.win.dnase.findAll(_ > 0).size > 0).map(_.label)
        .setName("yTrainPositive").cache()
      val yTrainNegatives = train.filter(r => r.win.dnase.findAll(_ > 0).size == 0).map(_.label)
        .setName("yTrainNegative").cache()
      val yTrain = yTrainPositives.union(yTrainNegatives).map(_.toDouble)
      
      // testing labels

      val yTestPositives = test.filter(r => r.win.dnase.findAll(_ > 0).size > 0).map(_.label)
        .setName("yTestPositive").cache()
      val yTestNegatives = test.filter(r => r.win.dnase.findAll(_ > 0).size == 0).map(_.label)
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

  def joinWithDnaseBams(sc: SparkContext,
                        sd: SequenceDictionary,
                        filteredRDD: RDD[LabeledWindow],
                        coverage: AlignmentRecordRDD): RDD[LabeledWindow] = {

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
      LeftOuterShuffleRegionJoin[LabeledWindow, AlignmentRecord](sd, 2000000, sc)
        .partitionAndJoin(filteredRDD.keyBy(_.win.region), mappedCoverage.rdd.keyBy(r => ReferenceRegion(r)))
        .filter(_._2.isDefined)
        .setName("cutsAndWindows")
        .cache()

    println(s"Final partition count: ${cutsAndWindows.partitions.length}")

    mappedCoverage.rdd.unpersist()
    filteredRDD.unpersist()

    val groupedWindows: RDD[(LabeledWindow, Iterable[AlignmentRecord])] = cutsAndWindows
      .groupBy(r => (r._1.win.region, r._1.win.getCellType, r._1.win.getTf))
      .map(r => {
        val x = r._2.toList
        (x.head._1, x.map(_._2.get).toIterable)
      })
      .setName("groupedWindows")
      .cache()

    println("Cuts and Windows Count: " + groupedWindows.count)
    cutsAndWindows.unpersist()

    // featurize datapoints to vector of numbers
    featurizePositives(groupedWindows, None, false)

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
   * @param cutsAndWindows cuts and windows to vectorize
   * @param doCentipede Whether or not to concolve using msCentipede algorithm
   * @param motifs
   * @return
   */
  def featurizePositives(cutsAndWindows: RDD[(LabeledWindow, Iterable[AlignmentRecord])],
                motifs: Option[List[Motif]] = None,
                doCentipede: Boolean = true): RDD[LabeledWindow] = {

    // filter windows into regions with and without relaxed dnase peaks

    // set size of dnase region to featurize
    val dnaseFeatureCount = 200
    val flanking = dnaseFeatureCount/2

    val centipedeWindows: RDD[LabeledWindow] =
      cutsAndWindows.map(window => {
        val cellType = window._1.win.cellType

        // where to center motif?
        val (start, end) =
          if (motifs.isDefined) { // if motif is defined center at most probable motif
          // format motifs to map on tf
          val tfmotifs = motifs.get.filter(_.label == window._1.win.tf.toString)
          val center =
            if (tfmotifs.length == 0) {
              println(s"no motifs for tf ${window._1.win.tf.toString}. Parsing from center")
              window._1.win.sequence.length/2
            } else {
              // get max motif location
                tfmotifs.map(motif => {
                  val motifLength = motif.length
                  // slide length, take index of max probabilitiy
                  window._1.win.sequence
                    .sliding(motifLength)
                    .map(r => motif.sequenceProbability(r))
                    .zipWithIndex
                    .maxBy(_._1)
                }).maxBy(_._1)._2
            }

            (window._1.win.region.start + center  - flanking,
              window._1.win.region.start + center + flanking) //determines size of dnase footprint region
          } else
            (window._1.win.region.start, window._1.win.region.end)

        val positivePositions = DenseVector.zeros[Int](Dataset.windowSize)
        val negativePositions = DenseVector.zeros[Int](Dataset.windowSize)

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


}


