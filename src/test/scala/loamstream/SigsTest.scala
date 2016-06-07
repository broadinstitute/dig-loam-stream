package loamstream

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.LSig
import org.scalatest.FunSuite

/**
  * @author clint
  *         date: Apr 26, 2016
  */
final class SigsTest extends FunSuite {

  test("Built-in sigs") {
    import Sigs._

    assert(variantAndSampleToGenotype =:= LSig.create[Map[(String, String), Genotype]])

    assert(sampleToSingletonCount =:= LSig.create[Map[String, Int]])

    assert(sampleIdAndIntToDouble =:= LSig.create[Map[(String, Int), Double]])
  }
}