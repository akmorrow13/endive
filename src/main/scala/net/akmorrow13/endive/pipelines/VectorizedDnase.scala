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
import net.akmorrow13.endive.utils._
import nodes.learning.LogisticRegressionEstimator
import org.apache.parquet.filter2.dsl.Dsl.{BinaryColumn, _}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{SequenceDictionary, ReferenceRegion}
import org.bdgenomics.formats.avro.Strand
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object VectorizedDnase extends Serializable  {

  /**
   * A very basic dataset creation pipeline that *doesn't* featurize the data
   * but creates a csv of (Window, Label)
   *
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
    val dnasePath = conf.dnase

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

    /************************************
      *  Prepare dnase data
    **********************************/
    val cuts: RDD[Cut] = Preprocess.loadCuts(sc, conf.dnase, cellTypes.toArray)
    println("Cuts: " + cuts.count)
assert(cuts.count > 0)
    val dnase = new Dnase(windowSize, stride, sc, cuts)
    val aggregatedCuts = dnase.merge(sd).cache()
    aggregatedCuts.count


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
      val train = featurize(sc, folds(i)._1.map(_._2), aggregatedCuts, sd, subselectNegatives = true).cache()
      val test = featurize(sc, folds(i)._2.map(_._2), aggregatedCuts, sd, subselectNegatives = false).cache()

      println("TRAIN SIZE IS " + train.count())
      println("TEST SIZE IS " + test.count())

      // training features
      val xTrainPositives = train.filter(r => r.features.findAll(_ > 0).size > 0).map(_.features)
        .setName("XTrainPositives").cache()
      val xTrainNegatives = train.map(_.features).filter(r => r.findAll(_ > 0).size == 0)
        .setName("XTrainNegatives").cache()
      println("training positives and negatives", xTrainPositives.count, xTrainNegatives.count)

      // testing features
      val xTestPositives: RDD[DenseVector[Double]] = test.map(_.features).filter(r => r.findAll(_ > 0).size > 0)
  .setName("XTestPositives").cache()
      val xTestNegatives: RDD[DenseVector[Double]] = test.map(_.features).filter(r => r.findAll(_ > 0).size == 0)
  .setName("XTestNegatives").cache()
      println("testing positives and negatives", xTestPositives.count, xTestNegatives.count)
 
      // training labels
      val yTrainPositives = train.filter(r => r.features.findAll(_ > 0).size > 0).map(_.labeledWindow.label)
        .setName("yTrainPositive").cache()
      val yTrainNegatives = train.filter(r => r.features.findAll(_ > 0).size == 0).map(_.labeledWindow.label)
        .setName("yTrainNegative").cache()
      val yTrain = yTrainPositives.union(yTrainNegatives).map(_.toDouble)
      
      // testing labels

      val yTestPositives = test.filter(r => r.features.findAll(_ > 0).size > 0).map(_.labeledWindow.label)
        .setName("yTestPositive").cache()
      val yTestNegatives = test.filter(r => r.features.findAll(_ > 0).size == 0).map(_.labeledWindow.label)
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

  def combineLabelWindowAndCutMap(labelwindow: Option[LabeledWindow], cutmap: Option[CutMap]): (LabeledWindow,CutMap) = {
    (labelwindow.orNull, cutmap.orNull)
  }

  /**
   * Featurize with full calcuated dnase coverage
   * @param sc SparkContext
   * @param rdd RDD of labeledWindow
   * @param coverage point coverage
   * @param sd SequenceDictionary
   * @param subselectNegatives whether or not to subselect the dataset
   * @return
   */
  def featurize(sc: SparkContext,
                rdd: RDD[LabeledWindow],
                coverage: RDD[CutMap],
                sd: SequenceDictionary,
                scale: Option[Int] = None,
                subselectNegatives: Boolean = true,
                 motifs: Option[List[Motif]] = None): RDD[BaseFeature] = {

    // get cell types in the current dataset
    val chromosomes = Chromosomes.toVector

    // set size of dnase region to featurize
    val windowSize = 200
    val windowJump = 50

    // perform optional negative sampling
    val filteredRDD =
      if (subselectNegatives)
        EndiveUtils.subselectSamples(sc, rdd, sd, partition = false)
      else
        rdd


    // filter windows into regions with and without relaxed dnase peaks
    val windowsWithoutDnase = filteredRDD.filter(_.win.dnase.size == 0)

    val windowsWithDnase = filteredRDD.filter(_.win.dnase.size > 0)
    println("Coverage: " + coverage.count)
    val regionsAndCuts: RDD[(ReferenceRegion, CutMap)] = coverage.flatMap (r => if(r.position.pos%windowJump == 0) { //occurs in windowSize/windowJump number of windows
                                                                                  for {
                                                                                    i <- 0 to windowSize/windowJump
                                                                                    if((r.position.pos / windowJump) * windowJump - (i * windowJump) > 0)
                                                                                  } yield
                                                                                      (ReferenceRegion(r.position.referenceName, (r.position.pos / windowJump) * windowJump - (i * windowJump), (r.position.pos / windowJump) * windowJump - (i * windowJump) + windowSize),r).asInstanceOf[(ReferenceRegion,CutMap)]
                                                                                } else { //occurs in windowSize/windowJump - 1 number of windows
                                                                                  for {
                                                                                    i <- 0 to windowSize/windowJump - 1
                                                                                    if((r.position.pos / windowJump) * windowJump - (i * windowJump) > 0)
                                                                                  } yield
                                                                                      (ReferenceRegion(r.position.referenceName, (r.position.pos / windowJump) * windowJump - (i * windowJump), (r.position.pos / windowJump) * windowJump - (i * windowJump) + windowSize),r).asInstanceOf[(ReferenceRegion,CutMap)]
                                                                                }
                                                                              )
    
    val cutsAndWindows = windowsWithDnase.map(f => (f.win.getRegion,f)).fullOuterJoin(regionsAndCuts).map(f => combineLabelWindowAndCutMap(f._2._1,f._2._2)).filter(f => f._1 != null && f._2 != null).groupByKey
    println("Cuts and Windows Count: " + cutsAndWindows.count)

    val centipedeWindows = featurizePositives(cutsAndWindows, scale, motifs)

    // put back in windows without relaxed dnase
    val featureLength = centipedeWindows.first.features.length
    windowsWithoutDnase.map(r => BaseFeature(r, DenseVector.zeros(featureLength))).union(centipedeWindows)

  }


  def featurizePositives(cutsAndWindows: RDD[(LabeledWindow, Iterable[CutMap])],
                scale: Option[Int] = None,
                motifs: Option[List[Motif]] = None): RDD[BaseFeature] = {

    // filter windows into regions with and without relaxed dnase peaks

    // set size of dnase region to featurize
    val dnaseFeatureCount = 200
    val flanking = dnaseFeatureCount/2

    val centipedeWindows: RDD[BaseFeature] =
      cutsAndWindows.map(windowed => {
        val window: LabeledWindow = windowed._1
        val cellType = window.win.cellType
        val dnase: Map[(Strand, Long), CutMap] = windowed._2.map(r => ((r.position.orientation, r.position.pos), r)).toMap

        // where to center motif?
        val (start, end) =
          if (motifs.isDefined) { // if motif is defined center at most probable motif
          // format motifs to map on tf
          val tfmotifs = motifs.get.filter(_.label == window.win.tf.toString)
          println("tfmotifs length", tfmotifs.length)
          val center =
            if (tfmotifs.length == 0) {
              println(s"no motifs for tf ${window.win.tf.toString}. Parsing from center")
              window.win.sequence.length/2
            } else {
              // get max motif location
                tfmotifs.map(motif => {
                  val motifLength = motif.length
                  // slide length, take index of max probabilitiy
                  window.win.sequence
                    .sliding(motifLength)
                    .map(r => motif.sequenceProbability(r))
                    .zipWithIndex
                    .maxBy(_._1)
                }).maxBy(_._1)._2
            }

            (window.win.region.start + center  - flanking,
              window.win.region.start + center + flanking) //determines size of dnase footprint region
          } else
            (window.win.region.start, window.win.region.end)


        val positivePositions: Array[Int] = (start until end)
          .map(r => {
            val m = dnase.get((Strand.FORWARD,r))
            if (m.isDefined) m.get.countMap.get(cellType).getOrElse(0)
            else 0
          }).toArray

        val negativePositions: Array[Int] = (start until end)
          .map(r => {
            val m = dnase.get((Strand.REVERSE,r))
            if (m.isDefined) m.get.countMap.get(cellType).getOrElse(0)
            else 0
          }).toArray

        val positions =
          Dnase.msCentipede(positivePositions, scale) ++ Dnase.msCentipede(negativePositions, scale)

        // positions should be size of window
        BaseFeature(window, DenseVector(positions))
      })
    centipedeWindows
  }


}


