package net.akmorrow13.endive.processing

import net.akmorrow13.endive.EndiveFunSuite
import org.bdgenomics.formats.avro.AlignmentRecord

class DnaseSuite extends EndiveFunSuite {

  val cellType = "A549"
  val id = "file1"

  sparkTest("should create 2 cuts from 1 AlignmentRecord") {
    val ar = AlignmentRecord.newBuilder()
              .setStart(10L)
              .setEnd(20L)
              .setReadNegativeStrand(false)
              .setContigName("chr1")
              .setReadName("myRead")
              .build()

    val cuts = Dnase.generateCuts(ar, cellType, id).toList

    assert(cuts.head.getStart == 10L && cuts.head.getEnd == 11L)
    assert(cuts.last.getStart == 20L && cuts.last.getEnd == 21L)

    val attrs = cuts.head.getAttributes.split(":")
    assert(attrs.head == cellType)


  }

  sparkTest("should create 2 cuts from 1 AlignmentRecord that is a negative strand") {
    val ar = AlignmentRecord.newBuilder()
      .setStart(10L)
      .setEnd(20L)
      .setReadNegativeStrand(true)
      .setContigName("chr1")
      .setReadName("myRead")
      .build()

    val cuts = Dnase.generateCuts(ar, cellType, id).toList
    assert(cuts.head.getStart == 9L && cuts.head.getEnd == 10L)
    assert(cuts.last.getStart == 19L && cuts.last.getEnd == 20L)

  }
}

