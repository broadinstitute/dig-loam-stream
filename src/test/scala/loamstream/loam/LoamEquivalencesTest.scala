package loamstream.loam

import loamstream.compiler.LoamPredef.store
import loamstream.loam.ops.StoreType.{TXT, VCF}
import org.scalatest.FunSuite
import loamstream.TestHelpers

/** Testing key slot equivalences */
final class LoamEquivalencesTest extends FunSuite {
  private implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty(TestHelpers.config))
  
  private val vcf1 = store[VCF]
  private val vcf2 = store[VCF]
  private val vcf3 = store[VCF]
  private val vcf4 = store[VCF]
  private val vcf5 = store[VCF]
  private val vcf6 = store[VCF]
  private val vcf7 = store[VCF]
  private val sampleMatrix = store[TXT]
  
  vcf1.key("sample").setSameListAs(vcf2.key("sample"))
  vcf2.key("sample").setSameListAs(vcf3.key("sample"))
  vcf3.key("sample").setSameSetAs(vcf4.key("sample"))
  vcf4.key("sample").setSameListAs(vcf5.key("sample"))
  vcf6.key("sample").setSameListAs(vcf7.key("sample"))
  
  sampleMatrix.key("sampleRow").setSameSetAs(vcf1.key("sample"))
  sampleMatrix.key("sampleColumn").setSameListAs(vcf7.key("sample"))

  private val slots = {
    Set(vcf1, vcf2, vcf3, vcf4, vcf5, vcf6, vcf7).map(_.key("sample")) ++
    Set(sampleMatrix.key("sampleRow"), sampleMatrix.key("sampleColumn"))
  }

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
    for {
      slot1 <- slots
      slot2 <- slots
    } {
      assert(!slot1.isSameListAs(slot2) || slot1.isSameSetAs(slot2))
    }
  }
}
