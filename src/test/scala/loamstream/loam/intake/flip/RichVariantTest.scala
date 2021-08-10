package loamstream.loam.intake.flip

import org.scalatest.FunSuite
import loamstream.loam.intake.Variant


/**
 * @author clint
 * Apr 1, 2020
 */
final class RichVariantTest extends FunSuite {
  private object JSet {
    def empty[A]: java.util.Set[A] = new java.util.HashSet 
    
    def apply[A](as: A*): java.util.Set[A] = {
      val result = new java.util.HashSet[A]
      
      as.foreach(result.add)
      
      result
    }
  }
  
  test("forwarded accessors") {
    val v = Variant.from(chrom = "a", pos = "123", alt = "G", ref = "c")
    
    val richV = new RichVariant(ReferenceFiles.empty, JSet.empty, v)
    
    assert(richV.chrom === "A")
    assert(richV.position === 123)
    assert(richV.alt === "G")
    assert(richV.reference === "C")
  }
  
  test("toKey") {
    val v = Variant.from(chrom = "a", pos = "123", alt = "g", ref = "c")
    
    val richV = new RichVariant(ReferenceFiles.empty, JSet.empty, v)
    
    assert(richV.toKey === s"A_123_C_G")
  }
  
  test("toKeyMunged") {
    val v = Variant.from(chrom = "a", pos = "123", alt = "A", ref = "C")
    
    val richV = new RichVariant(ReferenceFiles.empty, JSet.empty, v)
    
    assert(richV.toKeyComplemented === s"A_123_G_T")
  }
  
  test("isIn26k") {
    val v0 = Variant.from(chrom = "a", pos = "123", alt = "A", ref = "C")
    
    val v1 = Variant.from(chrom = "x", pos = "456", alt = "A", ref = "C")
    
    val varId0 = (new RichVariant(ReferenceFiles.empty, JSet.empty, v0)).toKey
    
    val varsIn26k: java.util.Set[String] = JSet(varId0) 
    
    val richV0 = new RichVariant(ReferenceFiles.empty, varsIn26k, v0)
    
    val richV1 = new RichVariant(ReferenceFiles.empty, varsIn26k, v1)
    
    assert(richV0.isIn26k === true)
    assert(richV1.isIn26k === false)
  }
  
  test("isIn26kMunged") {
    val v0 = Variant.from(chrom = "a", pos = "123", alt = "A", ref = "C")
    
    val v1 = Variant.from(chrom = "x", pos = "456", alt = "A", ref = "C")
    
    val varId0Munged = (new RichVariant(ReferenceFiles.empty, JSet.empty, v0)).toKeyComplemented
    
    val varsIn26k: java.util.Set[String] = JSet(varId0Munged) 
    
    val richV0 = new RichVariant(ReferenceFiles.empty, varsIn26k, v0)
    
    val richV1 = new RichVariant(ReferenceFiles.empty, varsIn26k, v1)
    
    assert(richV0.isIn26kComplemented === true)
    assert(richV1.isIn26kComplemented === false)
  }
  
  test("refChar") {
    Helpers.withTestFile("123456789") { testFile =>
      val handle = ReferenceFileHandle(testFile.toFile)
      
      val refFiles = new ReferenceFiles(Map("A" -> handle))
      
      val v0 = Variant.from(chrom = "a", pos = "2", alt = "A", ref = "C")
    
      val richV0 = new RichVariant(refFiles, JSet.empty, v0)
      
      val v1 = Variant.from(chrom = "a", pos = "123", alt = "A", ref = "C")
    
      val richV1 = new RichVariant(refFiles, JSet.empty, v1)
      
      val v2 = Variant.from(chrom = "Q", pos = "123", alt = "A", ref = "C")
    
      val richV2 = new RichVariant(refFiles, JSet.empty, v2)
      
      //NB: positions start from 1, so v0's position refers to the 2nd char in the file
      assert(richV0.refCharFromReferenceGenome === Some('2')) 
      assert(richV1.refCharFromReferenceGenome === None)
      assert(richV2.refCharFromReferenceGenome === None)
    }
  }
  
  test("refFromReferenceGenome") {
    Helpers.withTestFile("123456789") { testFile =>
      val handle = ReferenceFileHandle(testFile.toFile)
      
      val refFiles = new ReferenceFiles(Map("A" -> handle))
      
      val v0 = Variant.from(chrom = "a", pos = "2", alt = "A", ref = "CCT")
    
      val richV0 = new RichVariant(refFiles, JSet.empty, v0)
      
      val v1 = Variant.from(chrom = "a", pos = "123", alt = "A", ref = "C")
    
      val richV1 = new RichVariant(refFiles, JSet.empty, v1)
      
      val v2 = Variant.from(chrom = "Q", pos = "123", alt = "A", ref = "C")
    
      val richV2 = new RichVariant(refFiles, JSet.empty, v2)
      
      //NB: positions start from 1, so v0's position refers to the 2nd char in the file
      assert(richV0.refFromReferenceGenome === Some("234")) 
      assert(richV1.refFromReferenceGenome === None)
      assert(richV2.refFromReferenceGenome === None)
    }
  }
  
  test("altFromReferenceGenome") {
    Helpers.withTestFile("123456789") { testFile =>
      val handle = ReferenceFileHandle(testFile.toFile)
      
      val refFiles = new ReferenceFiles(Map("A" -> handle))
      
      val v0 = Variant.from(chrom = "a", pos = "3", alt = "ACT", ref = "CCT")
    
      val richV0 = new RichVariant(refFiles, JSet.empty, v0)
      
      val v1 = Variant.from(chrom = "a", pos = "123", alt = "A", ref = "C")
    
      val richV1 = new RichVariant(refFiles, JSet.empty, v1)
      
      val v2 = Variant.from(chrom = "Q", pos = "123", alt = "A", ref = "C")
    
      val richV2 = new RichVariant(refFiles, JSet.empty, v2)
      
      //NB: positions start from 1, so v0's position refers to the 2nd char in the file
      assert(richV0.altFromReferenceGenome === Some("345")) 
      assert(richV1.altFromReferenceGenome === None)
      assert(richV2.altFromReferenceGenome === None)
    }
  }
}
