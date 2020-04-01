package loamstream.loam.intake.flip

import org.scalatest.FunSuite
import loamstream.loam.intake.Variant


/**
 * @author clint
 * Apr 1, 2020
 */
final class RichVariantTest extends FunSuite {
  test("forwarded accessors") {
    val v = Variant(chrom = "a", pos = 123, alt = "b", ref = "c")
    
    val richV = new RichVariant(ReferenceFiles.empty, Set.empty, v)
    
    assert(richV.chrom === "a")
    assert(richV.position === 123)
    assert(richV.alt === "b")
    assert(richV.reference === "c")
  }
  
  test("toKey") {
    val v = Variant(chrom = "a", pos = 123, alt = "b", ref = "c")
    
    val richV = new RichVariant(ReferenceFiles.empty, Set.empty, v)
    
    assert(richV.toKey === s"a_123_b_c")
  }
  
  test("toKeyMunged") {
    val v = Variant(chrom = "a", pos = 123, alt = "A", ref = "C")
    
    val richV = new RichVariant(ReferenceFiles.empty, Set.empty, v)
    
    assert(richV.toKeyMunged === s"a_123_T_G")
  }
  
  test("isIn26k") {
    val v0 = Variant(chrom = "a", pos = 123, alt = "A", ref = "C")
    
    val v1 = Variant(chrom = "x", pos = 456, alt = "A", ref = "C")
    
    val varId0 = (new RichVariant(ReferenceFiles.empty, Set.empty, v0)).toKey
    
    val varsIn26k: Set[String] = Set(varId0) 
    
    val richV0 = new RichVariant(ReferenceFiles.empty, varsIn26k, v0)
    
    val richV1 = new RichVariant(ReferenceFiles.empty, varsIn26k, v1)
    
    assert(richV0.isIn26k === true)
    assert(richV1.isIn26k === false)
  }
  
  test("isIn26kMunged") {
    val v0 = Variant(chrom = "a", pos = 123, alt = "A", ref = "C")
    
    val v1 = Variant(chrom = "x", pos = 456, alt = "A", ref = "C")
    
    val varId0Munged = (new RichVariant(ReferenceFiles.empty, Set.empty, v0)).toKeyMunged
    
    val varsIn26k: Set[String] = Set(varId0Munged) 
    
    val richV0 = new RichVariant(ReferenceFiles.empty, varsIn26k, v0)
    
    val richV1 = new RichVariant(ReferenceFiles.empty, varsIn26k, v1)
    
    assert(richV0.isIn26kMunged === true)
    assert(richV1.isIn26kMunged === false)
  }
  
  test("refChar") {
    Helpers.withTestFile("123456789") { testFile =>
      val refFiles = new ReferenceFiles(Map("a" -> new ReferenceFileHandle(testFile.toFile)))
      
      val v0 = Variant(chrom = "a", pos = 2, alt = "A", ref = "C")
    
      val richV0 = new RichVariant(refFiles, Set.empty, v0)
      
      val v1 = Variant(chrom = "a", pos = 123, alt = "A", ref = "C")
    
      val richV1 = new RichVariant(refFiles, Set.empty, v1)
      
      val v2 = Variant(chrom = "Q", pos = 123, alt = "A", ref = "C")
    
      val richV2 = new RichVariant(refFiles, Set.empty, v2)
      
      //NB: positions start from 1, so v0's position refers to the 2nd char in the file
      assert(richV0.refChar === Some('2')) 
      assert(richV1.refChar === None)
      assert(richV2.refChar === None)
    }
  }
  
  test("refFromReferenceGenome") {
    Helpers.withTestFile("123456789") { testFile =>
      val refFiles = new ReferenceFiles(Map("a" -> new ReferenceFileHandle(testFile.toFile)))
      
      val v0 = Variant(chrom = "a", pos = 2, alt = "A", ref = "CCT")
    
      val richV0 = new RichVariant(refFiles, Set.empty, v0)
      
      val v1 = Variant(chrom = "a", pos = 123, alt = "A", ref = "C")
    
      val richV1 = new RichVariant(refFiles, Set.empty, v1)
      
      val v2 = Variant(chrom = "Q", pos = 123, alt = "A", ref = "C")
    
      val richV2 = new RichVariant(refFiles, Set.empty, v2)
      
      //NB: positions start from 1, so v0's position refers to the 2nd char in the file
      assert(richV0.refFromReferenceGenome === Some("234")) 
      assert(richV1.refFromReferenceGenome === None)
      assert(richV2.refFromReferenceGenome === None)
    }
  }
  
  test("altFromReferenceGenome") {
    Helpers.withTestFile("123456789") { testFile =>
      val refFiles = new ReferenceFiles(Map("a" -> new ReferenceFileHandle(testFile.toFile)))
      
      val v0 = Variant(chrom = "a", pos = 3, alt = "ACT", ref = "CCT")
    
      val richV0 = new RichVariant(refFiles, Set.empty, v0)
      
      val v1 = Variant(chrom = "a", pos = 123, alt = "A", ref = "C")
    
      val richV1 = new RichVariant(refFiles, Set.empty, v1)
      
      val v2 = Variant(chrom = "Q", pos = 123, alt = "A", ref = "C")
    
      val richV2 = new RichVariant(refFiles, Set.empty, v2)
      
      //NB: positions start from 1, so v0's position refers to the 2nd char in the file
      assert(richV0.altFromReferenceGenome === Some("345")) 
      assert(richV1.altFromReferenceGenome === None)
      assert(richV2.altFromReferenceGenome === None)
    }
  }
}
