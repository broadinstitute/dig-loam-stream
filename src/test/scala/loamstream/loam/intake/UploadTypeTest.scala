package loamstream.loam.intake

import org.scalatest.FunSuite
import scala.util.Success

/**
 * @author clint
 * 17 Mar, 2021
 */
final class UploadTypeTest extends FunSuite {
  import UploadType._
  
  test("name/s3Dir") {
    assert(Variants.name === "variants")
    assert(VariantCounts.name === "variant_counts")
    
    assert(Variants.s3Dir === "variants")
    assert(VariantCounts.s3Dir === "variant_counts")
  }
  
  test("fromString") {
    def doTestWorks(ut: UploadType): Unit = {
      assert(fromString(ut.name) === Some(ut))
      assert(fromString(ut.name.toUpperCase) === Some(ut))
      assert(fromString(ut.name.toLowerCase) === Some(ut))
      assert(fromString(ut.name.capitalize) === Some(ut))
    }
    
    def doTestDoesntWork(s: String): Unit = assert(fromString(s) === None)
    
    doTestWorks(Variants)
    doTestWorks(VariantCounts)
    
    doTestDoesntWork("")
    doTestDoesntWork("   ")
    doTestDoesntWork("alsfhjlasfkj")
  }
  
  test("tryFromString") {
    def doTestWorks(ut: UploadType): Unit = {
      assert(tryFromString(ut.name) === Success(ut))
      assert(tryFromString(ut.name.toUpperCase) === Success(ut))
      assert(tryFromString(ut.name.toLowerCase) === Success(ut))
      assert(tryFromString(ut.name.capitalize) === Success(ut))
    }
    
    def doTestDoesntWork(s: String): Unit = assert(tryFromString(s).isFailure)
    
    doTestWorks(Variants)
    doTestWorks(VariantCounts)
    
    doTestDoesntWork("")
    doTestDoesntWork("   ")
    doTestDoesntWork("alsfhjlasfkj")
  }
}