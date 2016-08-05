package loamstream.loam

import loamstream.compiler.LoamPredef.{TXT, VCF, store}
import loamstream.util.ValueBox
import org.scalatest.FunSuite

/** Testing key slot equivalences */
class LoamEquivalencesTest extends FunSuite {
  implicit val graphBox = new ValueBox(LoamGraph.empty)
  val vcf1 = store[VCF]
  val vcf2 = store[VCF]
  val vcf3 = store[VCF]
  val vcf4 = store[VCF]
  val vcf5 = store[VCF]
  val vcf6 = store[VCF]
  val vcf7 = store[VCF]
  val sampleMatrix = store[TXT]
  vcf1.key("sample").setSameListAs(vcf2.key("sample"))
  vcf2.key("sample").setSameListAs(vcf3.key("sample"))
  vcf3.key("sample").setSameSetAs(vcf4.key("sample"))
  vcf4.key("sample").setSameListAs(vcf5.key("sample"))
  vcf6.key("sample").setSameListAs(vcf7.key("sample"))
  sampleMatrix.key("sampleRow").setSameSetAs(vcf1.key("sample"))
  sampleMatrix.key("sampleColumn").setSameListAs(vcf7.key("sample"))

  val slots =
    Set(vcf1, vcf2, vcf3, vcf4, vcf5, vcf6, vcf7).map(_.key("sample")) ++
      Set(sampleMatrix.key("sampleRow"), sampleMatrix.key("sampleColumn"))

  test("Check selected key and list equivalences") {
    assert(vcf1.key("sample").isSameListAs(vcf3.key("sample")))
    assert(vcf1.key("sample").isSameSetAs(vcf3.key("sample")))
    assert(!vcf1.key("sample").isSameListAs(vcf5.key("sample")))
    assert(vcf1.key("sample").isSameSetAs(vcf5.key("sample")))
    assert(!vcf1.key("sample").isSameListAs(vcf7.key("sample")))
    assert(!vcf1.key("sample").isSameSetAs(vcf7.key("sample")))
    assert(vcf1.key("sample").isSameSetAs(sampleMatrix.key("sampleRow")))
    assert(vcf1.key("sample").isSameSetAs(sampleMatrix.key("sampleRow")))
  }

  test("Test that list equivalence implies set equivalence") {
    for(slot1 <- slots) {
      for(slot2 <- slots) {
        assert(!slot1.isSameListAs(slot2) || slot1.isSameSetAs(slot2))
      }
    }
  }
}
