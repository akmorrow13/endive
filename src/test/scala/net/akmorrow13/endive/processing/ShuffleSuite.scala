package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite
import scala.collection.mutable.ListBuffer

class ShuffleSuite extends EndiveFunSuite  {

 test("graph creation") {
    val testSeq = "AAACCC"
    val graph = Graph.form_seq_graph(testSeq)

    assert(graph('A').filter(_ =='A').length == 2)
    assert(graph('C').filter(_ =='A').length == 0)
    assert(graph('A').filter(_ =='C').length == 1)
    assert(graph('C').filter(_ =='C').length == 2)
 }

  test("generate shuffled sequence") {
    val testSeq =
      "CACACCGCACTCCCCAGCAGAAGGCTGCAATCCCACCTCTCTGATACAACCCTGCGCCTTGAGATGCAATCTAAACTAGGACTCTTGGTACCTTATCAAAC"
    val shuffled = DinucleotideShuffle.doublet_shuffle(testSeq)

    val strComp = "CAACTGGAGCTGATCTAGCCGACTACCTGCACTGTCTGCCACCCTCTTCTTCCCAACATGAGCTTACATAAACCCCATAAAGCATCAAGGGACTCCAACGC"

  }

}