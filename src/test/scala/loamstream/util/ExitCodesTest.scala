package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Mar 30, 2017
 */
final class ExitCodesTest extends FunSuite {
  //scalastyle:off magic.number
  
  test("isSuccess/isFailure") {
    import ExitCodes.{isSuccess, isFailure}
    
    assert(isSuccess(0))
    assert(isSuccess(-1) === false)
    assert(isSuccess(42) === false)
    assert(isSuccess(1000) === false)
    
    assert(isFailure(0) === false)
    assert(isFailure(-1))
    assert(isFailure(42))
    assert(isFailure(1000))
  }
  
  test("throwIfFailure") {
    import ExitCodes.throwIfFailure
    
    //Shouldn't throw
    throwIfFailure(0)
    
    val thrown = intercept[ExitCodeException] {
      throwIfFailure(42)
    }
    
    assert(thrown.exitCode === 42)
    assert(thrown.message.contains("42"))
    assert(thrown.message === thrown.getMessage)
  }
  
  test("throwIfFailure - custom exception") {
    import ExitCodes.throwIfFailure
    
    //Shouldn't throw
    throwIfFailure(0)
    
    val thrown = intercept[IllegalArgumentException] {
      throwIfFailure(42, (exitCode, message) => new IllegalArgumentException(s"blarg! $exitCode"))
    }
    
    assert(thrown.isInstanceOf[IllegalArgumentException])
    assert(thrown.getMessage === "blarg! 42")
  }
    
  //scalastyle:on magic.number
}
