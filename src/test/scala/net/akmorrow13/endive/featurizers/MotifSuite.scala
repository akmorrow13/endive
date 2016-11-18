package net.akmorrow13.endive.featurizers

import net.akmorrow13.endive.EndiveFunSuite

class MotifSuite extends EndiveFunSuite {

  // training data of region and labels
  var motifPath = resourcePath("models.yaml")

  test("should read pwms from yaml file") {
    val motifs = Motif.parseYamlMotifs(motifPath)
    assert(motifs.length == 3)
    val first = motifs.head.pwm
    assert(first(0) == 0.19882676005363464 && first(4) == 0.1623602658510208)
  }

}
