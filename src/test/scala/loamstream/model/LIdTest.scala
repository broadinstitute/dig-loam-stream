package loamstream.model

import org.scalatest.FunSuite

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
    
    def doTestWithRandomName(a: Long, b: Long) {
      val name = a + "_" + b
      
      if(a < 0 || b < 0) {
        assert(LId.fromName(name) == LNamedId(name))
      } else {
        assert(LId.fromName(name) == LAnonId(a, b))
      }
    }
    
    doTestWithNonRandomName("foo")
    doTestWithNonRandomName("foo_bar")
    doTestWithNonRandomName("123")
    doTestWithNonRandomName("123_bar")
    doTestWithNonRandomName("bar_123")
    doTestWithNonRandomName("fooaksjpa823045782j.sfmclajalhfr;'k;fl;jdf")
    doTestWithNonRandomName("") //TODO: Should we allow empty-string ids?
    
    doTestWithRandomName(123, 444)
    doTestWithRandomName(0, 0)
    doTestWithRandomName(Long.MaxValue, Long.MaxValue)
    doTestWithRandomName(Long.MaxValue, 0)
    doTestWithRandomName(0, Long.MaxValue)
    doTestWithRandomName(Long.MaxValue, 123)
    doTestWithRandomName(321, Long.MaxValue)
    
    doTestWithRandomName(0, Long.MinValue)
    doTestWithRandomName(Long.MinValue, 0)
    doTestWithRandomName(Long.MinValue, Long.MinValue)
    doTestWithRandomName(Long.MinValue, Long.MaxValue)
    doTestWithRandomName(Long.MaxValue, Long.MinValue)
  }
  
  test("name") {
    assert(LNamedId("foo").name == "foo")
    assert(LNamedId("").name == "") //TODO: Should we allow empty-string ids?
    
    assert(LAnonId(123, 456).name == "123_456")
  }
  
  test("Anon ID Invariants") {
    LAnonId(123, 456)
    LAnonId(0, 0)
    LAnonId(0, 123)
    LAnonId(123, 0)
    LAnonId(Long.MaxValue, 123)
    LAnonId(123, Long.MaxValue)
    LAnonId(Long.MaxValue, Long.MaxValue)
    
    intercept[Exception] {
      LAnonId(-1, 0)
      LAnonId(-1, -1)
      LAnonId(0, -1)
      
      LAnonId(Long.MinValue, Long.MaxValue)
      LAnonId(Long.MaxValue, Long.MinValue)
      LAnonId(Long.MinValue, Long.MinValue)
    }
  }
}