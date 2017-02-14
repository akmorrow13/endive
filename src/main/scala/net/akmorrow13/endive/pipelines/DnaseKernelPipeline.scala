/**
 * Copyright 2015 Vaishaal Shankar
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

import java.util.Random

import breeze.linalg._
import breeze.stats.distributions._
import net.akmorrow13.endive.EndiveConf
import net.akmorrow13.endive.featurizers.RandomDistribution
import net.akmorrow13.endive.metrics.Metrics
import net.akmorrow13.endive.processing._
import net.akmorrow13.endive.utils._
import com.github.fommil.netlib.BLAS
import net.jafama.FastMath
import nodes.akmorrow13.endive.featurizers.KernelApproximator
import nodes.learning.{BlockLinearMapper, LBFGSwithL2, BlockLeastSquaresEstimator}
import nodes.util.{Cacher, MaxClassifier, ClassLabelIndicatorsFromIntLabels}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
import org.bdgenomics.adam.rdd.feature.FeatureRDD
import org.bdgenomics.adam.util.TwoBitFile
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.io.LocalFileByteAccess
import org.yaml.snakeyaml.constructor.Constructor
import org.yaml.snakeyaml.Yaml
import pipelines.Logging
import org.apache.commons.math3.random.MersenneTwister
import java.io.{PrintWriter, File}



object DnaseKernelPipeline extends Serializable with Logging {

  /**
   * A very basic pipeline that *doesn't* featurize the data
   * simple regresses the raw sequence with the labels for the sequence
   *
   * HUGE CAVEATS
   * Trains a separate model for each TF type
   * Ignores cell type information
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
      conf.setIfMissing("spark.master", "local[4]")
      Logger.getLogger("org").setLevel(Level.INFO)
      Logger.getLogger("akka").setLevel(Level.INFO)
      val sc = new SparkContext(conf)
      val blasVersion = BLAS.getInstance().getClass().getName()
      println(s"Currently used version of blas is ${blasVersion}")
      run(sc, appConfig)
    }
  }


  def run(sc: SparkContext, conf: EndiveConf): Unit = {


    // set parameters
    val seed = 0
    val kmerSize = 8
    val approxDim = conf.approxDim
    val dnaseSize = 10
    val seqSize = 200
    val alphabetSize = Dataset.alphabet.size

    // generate headers
    val headers: Array[String] = sc.textFile(conf.deepSeaDataPath + "headers.csv").first().split(",")
    val labelHeaders = headers.zipWithIndex.filter(x => conf.deepSeaTfs.map(y => x._1 contains y).contains(true)).map(x => x._1)

    var (train, eval) = {
      if (conf.getAggregatedSequenceOutput == null) {
        val train = LabeledWindowLoader(s"${conf.getWindowLoc}_train", sc).setName("_All data")
        val eval = LabeledWindowLoader(s"${conf.getWindowLoc}_eval", sc).setName("_eval")
        (train, eval)
      } else {
        val train = MergeLabelsAndSequences.joinDnaseAndSequence(sc, s"${conf.getSequenceLoc}deepsea_train_reg_seq",
          s"${conf.getDnaseLoc}train/${conf.cellTypes}", s"${conf.getAggregatedSequenceOutput}${conf.cellTypes}_train", 200)
          .setName("_All data")

        val eval = MergeLabelsAndSequences.joinDnaseAndSequence(sc, s"${conf.getSequenceLoc}deepsea_eval_reg_seq",
          s"${conf.getDnaseLoc}eval/${conf.cellTypes}", s"${conf.getAggregatedSequenceOutput}${conf.cellTypes}_eval", 2)
          .setName("_eval")
        (train, eval)
      }
    }

    // Slice windows for 200 bp range
    train = train.repartition(700).cache().map(r => {
      val mid = r.win.getRegion.length /2 + r.win.getRegion.start
      val win =
        if (r.win.getDnase.length == 0) r.win.setDnase(DenseVector.zeros(r.win.getRegion.length.toInt))
        else r.win
      LabeledWindow(win.slice(mid-100,mid+100), r.labels)
    })
    train.count()
    println(train.first)
    eval = eval.repartition(2).cache().map(r => {
      val mid = r.win.getRegion.length /2 + r.win.getRegion.start
      val win =
        if (r.win.getDnase.length == 0) r.win.setDnase(DenseVector.zeros(r.win.getRegion.length.toInt))
        else r.win
      LabeledWindow(win.slice(mid-100,mid+100), r.labels)
    })
    eval.count
    println(eval.first)

    implicit val randBasis: RandBasis = new RandBasis(new ThreadLocalRandomGenerator(new MersenneTwister(seed)))
    val gaussian = new Gaussian(0, 1)

    // generate random matrix
    val W_sequence = DenseMatrix.rand(approxDim, kmerSize * alphabetSize, gaussian)

    // generate approximation features
    val (trainFeatures, evalFeatures) =
     if (conf.useDnase) {
       (featurizeWithDnase(sc, train, W_sequence, Array(kmerSize), dnaseSize, approxDim).map(_.features),
         featurizeWithDnase(sc, eval, W_sequence, Array(kmerSize), dnaseSize, approxDim).map(_.features))
     } else {
       (featurize(train, Array(W_sequence), Array(kmerSize)).map(_.features),
         featurize(eval, Array(W_sequence), Array(kmerSize)).map(_.features))
     }
 
    val trainLabels = train.map(_.labels.map(_.toDouble)).map(DenseVector(_))
    val evalLabels = eval.map(_.labels.map(_.toDouble)).map(DenseVector(_))

    trainFeatures.map(x => x.toArray.mkString(",")).zip(trainLabels).map(x => s"${x._1}|${x._2.toArray.mkString(",")}").saveAsTextFile(s"${conf.featurizedOutput}_${approxDim}_train")
    evalFeatures.map(x => x.toArray.mkString(",")).zip(evalLabels).map(x => s"${x._1}|${x._2.toArray.mkString(",")}").saveAsTextFile(s"${conf.featurizedOutput}_${approxDim}_eval")

    val model = new BlockLeastSquaresEstimator(approxDim, 1, conf.lambda).fit(trainFeatures, trainLabels)

    val allYTrain = model(trainFeatures)
    val allYEval = model(evalFeatures)

    val tfs = conf.tfs.split(',')

    val spots = headers.zipWithIndex.filter(r => !tfs.map(_.toString).filter(tf => r._1.contains(tf)).isEmpty)

  // score motifs
  val motifs =
    if (conf.motifDBPath != null) {
      Some(scoreMotifs(sc, tfs, conf.motifDBPath,
        W_sequence, model, kmerSize, seqSize))
    } else {
      None
    }

    // get metrics
    val scoreTfs = Array("ATF3","EGR1", "CEBPB","CTCF","GABP")
    printAllMetrics(headers, scoreTfs, allYTrain.zip(trainLabels), allYEval.zip(evalLabels), motifs)
  }

  def scoreMotifs(sc: SparkContext,
                  tfs: Array[String],
                  inputPath: String,
                  W_sequence: DenseMatrix[Double],
                  model: BlockLinearMapper,
                  kmerSize: Int,
                  seqSize: Int): RDD[(DenseVector[Double], Window)] = {

      // split raw text file of motifs
      val motifs = sc.textFile(inputPath)
        .map(r => (r.split(',')(0), r.split(',')(1))) // first is tf name, second is sequence
        .map(r => {
          val filled = seqSize - r._2.length
          val half = filled / 2
          val seq = randomSequence(half) + r._2 + randomSequence(filled - r._2.length - half)
          val win = Window(TranscriptionFactors.getEnumeration(r._1), CellTypes.Any, ReferenceRegion("chr1", 1, 200), seq)
          (r._2, LabeledWindow(win, 1))
        })

    val randomFeaturized =
      featurizeString(randomSequence(seqSize), Array(W_sequence), Array(kmerSize))
    val randomScore = model(randomFeaturized)

      // featurize motifs
      val featurized =
          featurize(motifs.map(_._2), Array(W_sequence), Array(kmerSize))

      model(featurized.map(r => r.features)).map(r =>  r / randomScore).zip(motifs.map(r => r._2.win.setSequence(r._1)))
  }

  /**
   * Generates a random DNA sequence of length
   * @param length length of random sequence
   * @return DNA random sequence
   */
  def randomSequence(length: Int): String = {
    val bases = Array('A','T','C','G')
    val random = scala.util.Random

    (0 until length).map(r => random.nextInt(bases.length))
      .map(r => bases(r)).toString()
  }

  /**
   * Prints metrics for train, eval and motifs
   * @param headers Headers straight
   * @param tfs
   * @param train
   * @param eval
   * @param motifs
   */
  def printAllMetrics(headers: Array[String],
            tfs: Array[String],
            train: RDD[(DenseVector[Double], DenseVector[Double])],
            eval: RDD[(DenseVector[Double], DenseVector[Double])],
            motifs: Option[RDD[(DenseVector[Double], Window)]]): Unit = {


    // only score specified TFs
    val spots = headers.zipWithIndex.filter(r => !tfs.filter(tf => r._1.contains(tf)).isEmpty)

    for (i <- spots) {
      val evalTrain = new BinaryClassificationMetrics(train.map(r => (r._1(i._2), r._2(i._2))))
      println(s"Train,${i._1},${i._2}")
      Metrics.printMetrics(evalTrain)

      val evalEval = new BinaryClassificationMetrics(eval.map(r => (r._1(i._2), r._2(i._2))))
      println(s"Eval,${i._1},${i._2}")
      Metrics.printMetrics(evalEval)

    }

    // motif metrics
    if (motifs.isDefined) {
      println("Evaluating Motifs")
      println(s"Motif\tSequence\t${spots.map(_._1).mkString("\t")}")
      val evalEval = motifs.get.collect
        .foreach(r => {
          val tfIndices = headers.zipWithIndex.filter(h => h._1.contains(r._2.getTf.toString)) // issue: headers
          val scores = r._1.toArray.zipWithIndex.filter(x => tfIndices.map(_._2).contains(x._2)).map(_._1)
          val scoresAndHeaders = tfIndices.map(_._1).zip(scores).map(x => (s"${x._1}:${x._2}")).mkString(",")
          println(s"${r._2.getTf}\t${r._2}\t${scoresAndHeaders}")
        })
    }
  }

  /**
   * Takes a region of the genome and one hot enodes sequences (eg A = 0001). If DNASE is enabled positive strands
   * are appended to the one hot sequence encoding. For example, if positive 1 has A with DNASE count of 3 positive
   * strands, the encoding is 0003.
   *
   * @param matrix: data matrix with sequences
   * @param Ws: random matrix
   * @param kmerSizes: length of all kmers to be created
   */
  def featurize(matrix: RDD[LabeledWindow],
                            Ws: Array[DenseMatrix[Double]],
                            kmerSizes: Array[Int]): RDD[FeaturizedLabeledWindow] = {
    require(Ws.length == kmerSizes.length, "W and kmers must be same length")

    val approximators = kmerSizes.zip(Ws).map(r  => {
      new KernelApproximator(r._2, FastMath.cos, r._1, Dataset.alphabet.size, fastfood=false)
    })

    matrix.map(f => {
      //iteratively compute and multiply all dnase for negative strands
      val kx = approximators.map(ap => {
        ap(KernelApproximator.stringToVector(f.win.getSequence))
      }).reduce(_ :* _)
      FeaturizedLabeledWindow(f, kx)
    })

  }

  /**
   * Takes a region of the genome and one hot enodes sequences (eg A = 0001). If DNASE is enabled positive strands
   * are appended to the one hot sequence encoding. For example, if positive 1 has A with DNASE count of 3 positive
   * strands, the encoding is 0003.
   *
   * @param l: sequence
   * @param Ws: random matrix
   * @param kmerSizes: length of all kmers to be created
   */
  def featurizeString(l: String,
                Ws: Array[DenseMatrix[Double]],
                kmerSizes: Array[Int]): DenseVector[Double] = {
    require(Ws.length == kmerSizes.length, "W and kmers must be same length")

    val approximators = kmerSizes.zip(Ws).map(r  => {
      new KernelApproximator(r._2, FastMath.cos, r._1, Dataset.alphabet.size, fastfood=false)
    })

    //iteratively compute and multiply all dnase for negative strands
    approximators.map(ap => {
      ap(KernelApproximator.stringToVector(l))
    }).reduce(_ :* _)

  }

  /**
   * Takes a region of the genome and one hot enodes sequences (eg A = 0001). If DNASE is enabled positive strands
   * are appended to the one hot sequence encoding. For example, if positive 1 has A with DNASE count of 3 positive
   * strands, the encoding is 0003.
   *
   * @param sc: Spark Context
   * @param rdd: data matrix with sequences
   * @return
   */
  def featurizeWithWaveletDnase(sc: SparkContext,
                         rdd: RDD[LabeledWindow],
                         W_sequence: DenseMatrix[Double],
                         kmerSize: Int, dnaseSize: Int,
                         approxDim: Int,
			 gamma: Double = 1.0): RDD[FeaturizedLabeledWindow] = {

    val kernelApprox_seq = new KernelApproximator(W_sequence, FastMath.cos, kmerSize, Dataset.alphabet.size, fastfood=false)

    val gaussian = new Gaussian(0, 1)

    // generate kernel approximators for all different variations of dnase length
    val approximators = Array(100, 50, 25, 12, 6)
      .map(r => {
        val W_pos = DenseMatrix.rand(approxDim, r, gaussian)
	val W_neg = DenseMatrix.rand(approxDim, r, gaussian)
        (new KernelApproximator(W_pos, FastMath.cos, r, 1, fastfood=false, gamma=gamma), new KernelApproximator(W_neg, FastMath.cos, r, 1, fastfood=false, gamma=gamma))
      })


    rdd.map(f => {

      val k_seq = kernelApprox_seq(KernelApproximator.stringToVector(f.win.getSequence))

      //iteratively compute and multiply all dnase for positive strands
      val k_dnase_pos = approximators.map(ap => {
        ap._1(f.win.getDnase.slice(0, Dataset.windowSize))
      }).reduce(_ :* _)

      //iteratively compute and multiply all dnase for negative strands
      val k_dnase_neg = approximators.map(ap => {
        ap._2(f.win.getDnase.slice(Dataset.windowSize, f.win.getDnase.length))
      }).reduce(_ :* _)

      FeaturizedLabeledWindow(f, DenseVector.vertcat(k_seq, k_dnase_pos, k_dnase_neg))
    })

  }

  /**
   * Takes a region of the genome and one hot enodes sequences (eg A = 0001). If DNASE is enabled positive strands
   * are appended to the one hot sequence encoding. For example, if positive 1 has A with DNASE count of 3 positive
   * strands, the encoding is 0003.
   *
   * @param sc: Spark Context
   * @param rdd: data matrix with sequences
   * @param kmerSizes size of all kmers
   * @param W_sequence random matrix for sequence
   * @param dnaseSize size of dnase
   * @return
   */
  def featurizeWithDnase(sc: SparkContext,
                         rdd: RDD[LabeledWindow],
                         W_sequence: DenseMatrix[Double],
                         kmerSizes: Array[Int],
                         dnaseSize: Int,
                         approxDim: Int,
			 gamma: Double = 1.0): RDD[FeaturizedLabeledWindow] = {

    val approximators_seq = kmerSizes.map(kmerSize => {
      new KernelApproximator(W_sequence, FastMath.cos, kmerSize, Dataset.alphabet.size, fastfood=false)
    })

    val gaussian = new Gaussian(0, 1)
    val W_dnase = DenseMatrix.rand(approxDim, dnaseSize, gaussian)

    val kernelApprox_dnase = new KernelApproximator(W_dnase, FastMath.cos, dnaseSize, 1, fastfood=false, gamma=gamma)

    rdd.map(f => {
      val k_dnase = kernelApprox_dnase(f.win.getDnase)
      val k_seq = approximators_seq.map(ap => {
        ap(KernelApproximator.stringToVector(f.win.getSequence))
      }).reduce(_ :* _)

      FeaturizedLabeledWindow(f, DenseVector.vertcat(k_seq, k_dnase))
    })
  }

  /**
   * One hot encodes sequences with dnase data
   * @param f feature with sequence and featurized dnase
   * @return Densevector of sequence and dnase mushed together
   */
  private[pipelines] def oneHotEncodeDnase(f: LabeledWindow): DenseVector[Double] = {

      // form seq of int from bases and join with dnase
      val intString: Seq[(Int, Double)] = f.win.getSequence.map(r => Dataset.alphabet.get(r).getOrElse(-1))
        .zip(f.win.getDnase.slice(0, Dataset.windowSize).toArray) // slice off just positives

      val seqString = intString.map { r =>
        val out = DenseVector.zeros[Double](Dataset.alphabet.size)
        if (r._1 != -1) {
          out(r._1) = 1 + r._2
        }
        out
      }
      DenseVector.vertcat(seqString: _*)
  }

}
