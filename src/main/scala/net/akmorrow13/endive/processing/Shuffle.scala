package net.akmorrow13.endive.processing

import net.akmorrow13.endive.utils.DeepbindRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random

import scala.collection.mutable.ListBuffer
import scala.util.Random

object DinucleotideShuffle {

  def doublet_shuffle(seq: String): String = {
    val last_nt = seq.toCharArray.last
    val graph = Graph.form_seq_graph(seq)
    // sample a random last edge graph
    var is_ok = false
    var le_graph: Map[Char, List[Char]] = Map()
    while (!is_ok) {
      le_graph = Graph.sample_le_graph(graph, last_nt)
      // check the last edge graph
      is_ok = Graph.check_le_graph(le_graph, last_nt)
    }
    val new_graph = Graph.form_new_graph(graph, le_graph, last_nt)
    val shuf_seq  = Graph.form_shuffled_seq(new_graph.mapValues(_.toList), seq(0), seq.length)
    return shuf_seq

  }

  def getShuffledDinucletides(rdd: RDD[DeepbindRecord]): RDD[DeepbindRecord] = {

    rdd.map(_.sequence).map(r => doublet_shuffle(r))
      .zip(rdd)
      .map(r => DeepbindRecord(r._2.tf, r._2.cellType, r._2.id +"shuff", r._1, 0))

  }


}

object Graph {

   def form_shuffled_seq(new_graph: Map[Char, List[Char]], init_nt: Char, len_seq: Int): String = {
     var tmp_graph = new_graph
     var is_done = false
     var new_seq: ListBuffer[Char] = ListBuffer(init_nt)
     while (!is_done) {
       val last_nt = new_seq.last
       try {
         new_seq += tmp_graph(last_nt)(0)
       } catch {
         // TODO: this is not quite right
         case e: Exception => new_seq ++= tmp_graph.values.flatten.toList
       }

       // pop off last element
       tmp_graph = tmp_graph + (last_nt -> tmp_graph(last_nt).drop(1))
       if(new_seq.length >= len_seq)
         is_done = true
     }
     return new_seq.mkString
   }

  // sample a random last edge
  def sample_le_graph(graph: Map[Char, List[Char]], last_nt: Char): Map[Char, List[Char]] = {
    var newGraph: Map[Char, List[Char]] = Map()

    graph.map(r => {
      val newList =
        if (r._1 != last_nt) {
          val x = Random.nextInt(graph(r._1).size)
          newGraph += (r._1 -> List(graph(r._1)(x)))
        } else List()
      (r._1, r._2.toList)
    })
  }

  def check_le_graph(le_graph: Map[Char, List[Char]], last_nt: Char): Boolean = {
    for (k <- le_graph.keys) {
      if (k != last_nt) {
        if (!find_path(le_graph, k, last_nt).isDefined)
          return false
      }
    }
    true
  }

  // check whether there is a path between two nodes in a graph
  def find_path(graph: Map[Char, List[Char]], start: Char, end: Char, path: ListBuffer[Char] = ListBuffer.empty): Option[ListBuffer[Char]] = {
    path += start
    if (start == end) {
      return Some(path)
    }
    if (!graph.keySet.contains(start)) {
      return None
    }
    for (node <- graph(start)) {
      if (!path.contains(node)) {
        val newPath = find_path(graph, node, end, path)
        if (newPath.isDefined) return newPath
      }
      return None
    }
    None
  }

  def form_new_graph(graph: Map[Char, List[Char]], le_graph: Map[Char, List[Char]], last_nt: Char): Map[Char, ListBuffer[Char]] = {
    var new_graph: Map[Char, ListBuffer[Char]] = Map[Char, ListBuffer[Char]]()
    for (vx <- graph.keys) {
      new_graph += (vx -> ListBuffer[Char]())
      var temp_edges = graph(vx)
      if (vx != last_nt)
        temp_edges = Random.shuffle(temp_edges.drop(1))
      for (ux <- temp_edges)
        new_graph(vx).append(ux)
      if (vx != last_nt)
        new_graph(vx).append(le_graph(vx)(0))
    }
    new_graph
  }

  // for each base, keep track of the bases you see after that base
  def form_seq_graph(seq: String): Map[Char, List[Char]] = {

    var graph: Map[Char, ListBuffer[Char]] = Map[Char, ListBuffer[Char]]()

    val arr = seq.toCharArray

    for (i <- 0 until arr.length-1) {
      if (!graph.get(arr(i)).isDefined) {
        graph += (arr(i) -> ListBuffer())
      }
      val charList: ListBuffer[Char] = graph.get(arr(i)).get // get base in graph
      // update map
      val char =arr(i+1)
      charList += char

      graph = graph + (arr(i) -> charList)
    }
    graph.mapValues(_.toList)
  }
//
//  def empty: Map[Char, ListBuffer[Char]] = {
//    Map()
////    Map('A'-> ListBuffer(), 'C'->ListBuffer(),'G'->ListBuffer(),'T'->ListBuffer())
//  }

}