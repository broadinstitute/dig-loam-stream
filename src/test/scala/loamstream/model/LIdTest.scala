package loamstream.model

import org.scalatest.FunSuite

/**
 * @author clint
 * date: May 23, 2016
 */
final class LIdTest extends FunSuite {
  test("fromName") {
    def doTestWithNonRandomName(n: String) {
      assert(LId.fromName(n) == LId.LNamedId(n))
    }
    
    def doTestWithRandomName(a: Long, b: Long) {
      val name = a + "_" + b
      
      if(a < 0 || b < 0) {
        assert(LId.fromName(name) == LId.LNamedId(name))
      } else {
        assert(LId.fromName(name) == LId.LAnonId(a, b))
      }
    }
    
    doTestWithNonRandomName("foo")
    doTestWithNonRandomName("foo_bar")
    doTestWithNonRandomName("123")
    doTestWithNonRandomName("123_bar")
    doTestWithNonRandomName("bar_123")
    doTestWithNonRandomName("fooaksjpa823045782j.sfmclajalhfr;'k;fl;jdf")
    doTestWithNonRandomName("") //TODO: Should we allow empty names?
    
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
}