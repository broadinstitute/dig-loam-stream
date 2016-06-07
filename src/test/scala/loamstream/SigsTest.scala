package loamstream

import htsjdk.variant.variantcontext.Genotype
import org.scalatest.FunSuite

import scala.reflect.runtime.universe.typeOf

/**
  * @author clint
  *         date: Apr 26, 2016
  */
final class SigsTest extends FunSuite {

  test("Built-in sigs") {
    import Sigs._

    assert(variantAndSampleToGenotype =:= typeOf[Map[(String, String), Genotype]])

    assert(sampleToSingletonCount =:= typeOf[Map[String, Int]])

    assert(sampleIdAndIntToDouble =:= typeOf[Map[(String, Int), Double]])
  }
}