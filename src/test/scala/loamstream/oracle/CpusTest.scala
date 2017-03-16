package loamstream.oracle

import org.scalatest.FunSuite

/**
 * @author clint
 * Mar 13, 2017
 */
final class CpusTest extends FunSuite {
  //scalastyle:off magic.number
  
  test("Guards") {
    intercept[Exception] {
      Cpus(0)
    }
    
    intercept[Exception] {
      Cpus(-1)
    }
    
    intercept[Exception] {
      Cpus(-100)
    }
  }
  
  test("isSingle") {
    assert(Cpus(1).isSingle)
    
    assert(Cpus(42).isSingle === false)
  }
  //scalastyle:on magic.number
}
