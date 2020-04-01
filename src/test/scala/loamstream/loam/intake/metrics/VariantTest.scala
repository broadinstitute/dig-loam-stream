package loamstream.loam.intake.metrics

import org.scalatest.FunSuite
import loamstream.loam.intake.Variant

/**
 * @author clint
 * Apr 1, 2020
 */
final class VariantTest extends FunSuite {
  test("from") {
    import Variant.from
    
    assert(from("x_123_y_z") === Variant(chrom = "x", pos = 123, alt = "y", ref = "z"))
    assert(from("x 123 y z") === Variant(chrom = "x", pos = 123, alt = "y", ref = "z"))
    assert(from("x:123:y:z") === Variant(chrom = "x", pos = 123, alt = "y", ref = "z"))
    
    intercept[Exception] {
      from("x123yz")
      from("asdf")
      from("")
      from("x_123 y:z")
    }
  }
  
  test("unapply") {
    import Variant.unapply
    
    assert(unapply("x_123_y_z") === Some(Variant(chrom = "x", pos = 123, alt = "y", ref = "z")))
    assert(unapply("x 123 y z") === Some(Variant(chrom = "x", pos = 123, alt = "y", ref = "z")))
    assert(unapply("x:123:y:z") === Some(Variant(chrom = "x", pos = 123, alt = "y", ref = "z")))
    
    assert(unapply("x123yz") === None)
    assert(unapply("asdf") === None)
    assert(unapply("") === None)
    assert(unapply("x_123 y:z") === None)
  }
  
  test("asBioIndexCoord") {
    val v0 = Variant(chrom = "x", pos = 123, alt = "y", ref = "z")
    
    assert(v0.asBioIndexCoord === "chrx:123")
    
    //same chrom/pos, different ref/alt 
    val v1 = Variant(chrom = "x", pos = 123, alt = "a", ref = "b")
    
    assert(v1.asBioIndexCoord === "chrx:123")
    
    val v2 = Variant(chrom = "ABC", pos = 456, alt = "a", ref = "b")
    
    assert(v2.asBioIndexCoord === "chrABC:456")
  }
  
  test("flip") {
    val v0 = Variant(chrom = "x", pos = 123, alt = "y", ref = "z")
    
    assert(v0.alt !== v0.ref)
    
    assert(v0.flip.flip === v0)
    
    assert(v0.flip === Variant(chrom = "x", pos = 123, alt = "z", ref = "y"))
  }
  
  test("delimitedBy/colonDelimited/underscoreDelimited") {
    val v0 = Variant(chrom = "x", pos = 123, alt = "y", ref = "z")
    
    assert(v0.colonDelimited === "x:123:y:z")
    assert(v0.underscoreDelimited === "x_123_y_z")
    assert(v0.delimitedBy('>') === "x>123>y>z")
  }
}
