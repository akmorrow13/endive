package net.akmorrow13.endive.pipelines

import breeze.linalg.DenseVector
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.Motif
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing.Dataset.{CellTypes, Chromosomes}
import net.akmorrow13.endive.utils._
import nodes.learning.LogisticRegressionEstimator

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.SequenceDictionary
import org.bdgenomics.adam.rdd.GenomicRegionPartitioner
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import net.akmorrow13.endive.processing._


object CSKPipeline extends Serializable  {

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
    println("STARTING BASEMODEL PIPELINE")

    // challenge parameters
    val windowSize = 200
    val stride = 50


    // create new sequence with reference path
    val referencePath = conf.reference
    // load chip seq labels from any number of files
    val labelsPathArray = conf.labels.split(" ")
    val dnasePath = conf.dnase
    val motifPath = conf.motifDBPath
    val cutmapInputPath = conf.cutmapInputPath
    val cutmapOutputPath = conf.cutmapOutputPath
    val predictionOutputPath = conf.predictionOutputPath
    //this should be a boolean
    var modelTest = true
    try{
      modelTest = conf.modelTest.toBoolean
      if(modelTest == false){
        require(predictionOutputPath != null, "You must specify output path for full prediction")
      }
    } catch {
      case e: java.lang.IllegalArgumentException => modelTest = true
    }

    /** ***************************************
      * Read in reference dictionary
      *****************************************/
    val records = DatasetCreationPipeline.getSequenceDictionary(referencePath)
      .records.filter(r => Chromosomes.toVector.contains(r.name))

    val sd = new SequenceDictionary(records)

    val motifs: List[Motif] = Motif.parseYamlMotifs(motifPath)

    var data: RDD[LabeledWindow] = sc.emptyRDD[LabeledWindow]
    for(label <- labelsPathArray){
      data = data.union(sc.textFile(label).map(s => LabeledWindowLoader.stringToLabeledWindow(s)))
    }

    val windowsRDD = data.setName("windowsRDD").cache()

    val chrCellTypes:Iterable[(String, CellTypes.Value)] = windowsRDD.map(x => (x.win.getRegion.referenceName, x.win.cellType)).countByValue().keys
    val cellTypes = chrCellTypes.map(_._2)

    /************************************
      *  Prepare dnase data
      **********************************/
    var aggregatedCuts: RDD[CutMap] = sc.emptyRDD[CutMap]
    try {
      aggregatedCuts = sc.objectFile(cutmapInputPath)
      aggregatedCuts.count
    } catch {
      case e @ (_: org.apache.hadoop.mapred.InvalidInputException | _: java.lang.NullPointerException) => {
        val keyedCuts = Preprocess.loadCuts(sc, conf.dnase, cellTypes.toArray)
          .keyBy(r => (r.region, r.getCellType))
          .partitionBy(GenomicRegionPartitioner(1000, sd))
        keyedCuts.count
        val cuts = keyedCuts.map(_._2)
        cuts.count
        val dnase = new Dnase(windowSize, stride, sc, cuts)
        aggregatedCuts = dnase.merge(sd).cache()
        aggregatedCuts.count
        try {
          aggregatedCuts.saveAsObjectFile(cutmapOutputPath)
          println("Wrote cutmap to " + cutmapOutputPath)
        } catch {
          case e @ (_: java.lang.NullPointerException | _: java.lang.IllegalArgumentException) => {
            println("To store Cutmap, add {cutmapOutputPath} to conf file")
          }
        }
      }
    }


    val featurized = VectorizedDnase.featurize(sc, windowsRDD, aggregatedCuts, sd, None, false, motifs=Some(motifs))
    val folds: IndexedSeq[(RDD[((String, CellTypes.Value), BaseFeature)], RDD[((String, CellTypes.Value), BaseFeature)])] =
    if(modelTest) {
      /* First one chromesome and one celltype per fold (leave 1 out) */
        EndiveUtils.generateFoldsRDD(featurized.keyBy(r => (r.labeledWindow.win.region.referenceName, r.labeledWindow.win.cellType)), conf.heldOutCells, conf.heldoutChr, conf.folds, sampleFreq = None)
    } else{
      //We have to wrap this in a vector because we want to reuse as much logic as possible.
      IndexedSeq(EndiveUtils.generateTrainTestSplit(featurized.keyBy(r => (r.labeledWindow.win.region.referenceName, r.labeledWindow.win.cellType)), Dataset.heldOutTypes.toSet.map(CellTypes.getEnumeration)))
    }

    for (i <- (0 until folds.size)) {
      val r = new java.util.Random()
      var train = folds(i)._1.map(_._2)

      train = train.filter(x => x.labeledWindow.label == 1 || (x.labeledWindow.label == 0 && r.nextFloat < 0.001))
      train.setName("train").cache()
      val test = folds(i)._2.map(_._2)
        .setName("test").cache()

      val xTrain = train.map(_.features)
      val xTest = test.map(_.features)

      val yTrain = train.map(_.labeledWindow.label)
      val yTest = test.map(_.labeledWindow.label)

      println("TRAIN SIZE IS " + train.count())
      println("TEST SIZE IS " + test.count())

      // get testing cell types for this fold
      val cellTypesTest: Iterable[CellTypes.Value] = test.map(x => (x.labeledWindow.win.cellType)).countByValue().keys
      println(s"Fold " + i + " testing cell types:")
      cellTypesTest.foreach(println)

      // get testing chrs for this fold
      val chromosomesTest:Iterable[String] = test.map(x => (x.labeledWindow.win.getRegion.referenceName)).countByValue().keys
      println("Fold " + i + " testing chromsomes:")
      chromosomesTest.foreach(println)

      println("Training model")
      val predictor = LogisticRegressionEstimator[DenseVector[Double]](numClasses = 2, numIters = 10, regParam=0.01)
        .fit(xTrain, yTrain)

      val yPredTrain = predictor(xTrain)
      val evalTrain = new BinaryClassificationMetrics(yPredTrain.zip(yTrain.map(_.toDouble)))
      println("Train Results: \n ")
      Metrics.printMetrics(evalTrain)

      val yPredTest = predictor(xTest)
      val finalPrediction = test.map(f => (f.labeledWindow.win.region, f.labeledWindow.win.cellType)).zip(yPredTest)
      if(modelTest == false) {
        //wouldn't it be sad if we made it here and realized that our path already existed?
        //most of the logic here is to prevent sadness and wasted time.
        try {
          test.map(f => (f.labeledWindow.win.region, f.labeledWindow.win.cellType)).zip(yPredTest).saveAsTextFile(predictionOutputPath)
        } catch {
          case e: org.apache.hadoop.mapred.FileAlreadyExistsException => {
            var i = 0
            while(true) {
              try {
                finalPrediction.saveAsTextFile(predictionOutputPath + "_" + i)
                println("Filename already exists, storing as " + predictionOutputPath + "_" + i)
                sys.exit()
              } catch {
                case e: org.apache.hadoop.mapred.FileAlreadyExistsException => {
                  i+=1
                }
              }
            }
          }
        }
        sys.exit()
      }
      val evalTest = new BinaryClassificationMetrics(yPredTest.zip(yTest.map(_.toDouble)))
      println("Test Results: \n ")
      Metrics.printMetrics(evalTest)
      train.unpersist(true)
      test.unpersist(true)
    }
  }
}
