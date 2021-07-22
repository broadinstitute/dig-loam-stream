package loamstream.loam.intake

import org.scalatest.FunSuite

/**
  * @author clint
  * @date Jul 21, 2021
  *
  */
final class AllelesTest extends FunSuite {
  test("isAllowedAllele") {
    import Alleles.isAllowedAllele
    
    assert(isAllowedAllele('A'))
    assert(isAllowedAllele('G'))
    assert(isAllowedAllele('C'))
    assert(isAllowedAllele('T'))

    assert(isAllowedAllele('a'))
    assert(isAllowedAllele('g'))
    assert(isAllowedAllele('c'))
    assert(isAllowedAllele('t'))

    val r = new scala.util.Random

    val agct: Set[Char] = "AGCTagct".toSet

    def randomFailingChar: Char = Iterator.continually(r.nextPrintableChar).filterNot(agct).next()

    assert(isAllowedAllele(randomFailingChar) === false)
    assert(isAllowedAllele(randomFailingChar) === false)
    assert(isAllowedAllele(randomFailingChar) === false)
    assert(isAllowedAllele(randomFailingChar) === false)
  }

  test("areAllowedAlleles") {
    import Alleles.areAllowedAlleles

    assert(areAllowedAlleles("") === false)
    assert(areAllowedAlleles("   ") === false)
    assert(areAllowedAlleles("asdf") === false)
    assert(areAllowedAlleles("ASDF") === false)

    assert(areAllowedAlleles("a"))
    assert(areAllowedAlleles("g"))
    assert(areAllowedAlleles("c"))
    assert(areAllowedAlleles("t"))

    assert(areAllowedAlleles("agct"))
    assert(areAllowedAlleles("a,g,c,t"))

    assert(areAllowedAlleles("A"))
    assert(areAllowedAlleles("G"))
    assert(areAllowedAlleles("C"))
    assert(areAllowedAlleles("T"))
    assert(areAllowedAlleles("AGCT"))
    assert(areAllowedAlleles("TG"))
    assert(areAllowedAlleles("CA"))
    assert(areAllowedAlleles("A,G,C,T"))
    assert(areAllowedAlleles("A,C"))
    assert(areAllowedAlleles("G,T"))
  }
}
