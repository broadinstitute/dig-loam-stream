package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * Apr 1, 2020
 */
final class VariantTest extends FunSuite {
  
  test("complement") {
    assert(Variant.from("1", "123", "A", "C").complement === Variant.from("1", "123", "T", "G"))
    assert(Variant.from("1", "123", "T", "G").complement === Variant.from("1", "123", "A", "C"))
    
    def doRoundTrip(v: Variant): Unit = {
      assert(v.complement.complement === v) 
    }
    
    doRoundTrip(Variant.from("1", "123", "A", "C"))
    doRoundTrip(Variant.from("1", "123", "T", "G"))
  }
  
  test("toUpperCase") {
    val v = Variant.from(chrom = "xwYE", pos = "123", alt = "atC", ref = "gGc")
    
    val expected = "XWYE_123_GGC_ATC"
    
    assert(v.toUpperCase.underscoreDelimited === expected)
    
    assert(v.toUpperCase.toUpperCase.underscoreDelimited === expected)
  }
  
  test("from") {
    import Variant.from
    
    assert(from("x_123_g_c") === Variant.from(chrom = "X", pos = "123", alt = "C", ref = "G"))
    assert(from("x 123 g c") === Variant.from(chrom = "X", pos = "123", alt = "C", ref = "G"))
    assert(from("x:123:g:c") === Variant.from(chrom = "X", pos = "123", alt = "C", ref = "G"))
    
    intercept[Exception] { from("x123yz") }
    intercept[Exception] { from("asdf") }
    intercept[Exception] { from("") }
    intercept[Exception] { from("x_123 y:z") }
    intercept[Exception] { from("x:123:lol:nuh") }
  }
  
  test("unapply") {
    import Variant.unapply
    
    assert(unapply("x_123_g_c") === Some(Variant.from(chrom = "X", pos = "123", alt = "C", ref = "G")))
    assert(unapply("x 123 g c") === Some(Variant.from(chrom = "X", pos = "123", alt = "C", ref = "G")))
    assert(unapply("x:123:g:c") === Some(Variant.from(chrom = "X", pos = "123", alt = "C", ref = "G")))
    
    assert(unapply("x123yz") === None)
    assert(unapply("asdf") === None)
    assert(unapply("") === None)
    assert(unapply("x_123 y:z") === None)
    assert(unapply("x:123:lol:nuh") === None)
  }
  
  test("asBioIndexCoord") {
    val v0 = Variant.from(chrom = "x", pos = "123", alt = "T", ref = "A")
    
    assert(v0.asBioIndexCoord === "chrX:123")
    
    val v2 = Variant.from(chrom = "ABC", pos = "456", alt = "a", ref = "t")
    
    assert(v2.asBioIndexCoord === "chrABC:456")
  }
  
  test("asFullBioIndexCoord") {
    val v0 = Variant.from(chrom = "x", pos = "123", alt = "G", ref = "C")
    
    assert(v0.asFullBioIndexCoord === "chrX:123:C:G")
    
    //same chrom/pos, different ref/alt 
    val v1 = Variant.from(chrom = "x", pos = "123", alt = "G", ref = "C")
    
    assert(v1.asFullBioIndexCoord === "chrX:123:C:G")
    
    val v2 = Variant.from(chrom = "ABC", pos = "456", alt = "G", ref = "C")
    
    assert(v2.asFullBioIndexCoord === "chrABC:456:C:G")
  }
  
  test("flip") {
    val v0 = Variant.from(chrom = "x", pos = "123", alt = "G", ref = "C")
    
    assert(v0.alt !== v0.ref)
    
    assert(v0.flip.flip === v0)
    
    assert(v0.flip === Variant.from(chrom = "x", pos = "123", alt = "C", ref = "G"))
  }
  
  test("delimitedBy/colonDelimited/underscoreDelimited") {
    val v0 = Variant.from(chrom = "x", pos = "123", alt = "g", ref = "c")
    
    assert(v0.colonDelimited === "X:123:C:G")
    assert(v0.underscoreDelimited === "X_123_C_G")
    assert(v0.delimitedBy('>') === "X>123>C>G")
  }
  
  test("isMultiAllelic") {
    //def isMultiAllelic: Boolean = alt.split(',').size > 1
    
    val notMultiAllelic = Variant.from(chrom = "x", pos = "123", alt = "G", ref = "C")
    
    assert(notMultiAllelic.isMultiAllelic === false)
    
    val multiAllelicAlt = Variant.from(chrom = "x", pos = "123", alt = "g,c,t", ref = "a")
    
    assert(multiAllelicAlt.isMultiAllelic === true)

    val multiAllelicRef = Variant.from(chrom = "x", pos = "123", alt = "a", ref = "g,c,t")
    
    assert(multiAllelicRef.isMultiAllelic === true)
  }
}
