package loamstream.model

import org.scalatest.FunSuite

import scala.collection.compat._

/**
 * @author clint
 * date: May 23, 2016
 */
final class LIdTest extends FunSuite {
  import LId.{LNamedId, LAnonId}
  
  test("fromName") {
    def doTestWithNonRandomName(n: String) {
      assert(LId.fromName(n) == LNamedId(n))
    }
    
    def doTestWithRandomName(a: Long) {
      val name = "anon$" + a
      
      if(a < 0) {
        assert(LId.fromName(name) == LNamedId(name))
      } else {
        assert(LId.fromName(name) == LAnonId(a))
      }
    }
    
    doTestWithNonRandomName("foo")
    doTestWithNonRandomName("foo_bar")
    doTestWithNonRandomName("123")
    doTestWithNonRandomName("123_bar")
    doTestWithNonRandomName("bar_123")
    doTestWithNonRandomName("fooaksjpa823045782j.sfmclajalhfr;'k;fl;jdf")
    doTestWithNonRandomName("") //TODO: Should we allow empty-string ids?
    
    doTestWithRandomName(123) // scalastyle:ignore magic.number
    doTestWithRandomName(0)
    doTestWithRandomName(Long.MaxValue)
        
    doTestWithRandomName(Long.MinValue)
    doTestWithRandomName(-1)
  }
  
  test("name") {
    assert(LNamedId("foo").name == "foo")
    assert(LNamedId("").name == "") //TODO: Should we allow empty-string ids?
    
    assert(LAnonId(123).name == "$123") // scalastyle:ignore magic.number
  }
  
  test("Anon ID Invariants") {
    // scalastyle:off magic.number
    LAnonId(123)
    LAnonId(0)
    LAnonId(Long.MaxValue)
    // scalastyle:on magic.number
    intercept[Exception] {
      LAnonId(-1)
      LAnonId(Long.MinValue)
    }
  }
  
  test("New anon ids") {
    import LId.newAnonId
    
    assert(newAnonId != newAnonId)
    assert(newAnonId != newAnonId)
    assert(newAnonId != newAnonId)
    
    val n = 10
    
    val ids = (0 until n).map(_ => newAnonId)
    
    assert(ids.size == n)
    assert(ids.to(Set).size == n)
  }
}
