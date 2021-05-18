package loamstream.util

import org.scalatest.FunSuite

/**
 * @author clint
 * Jul 9, 2019
 */
final class RegexesTest extends FunSuite {
  import Regexes.Implicits._
  
  val letters = "[a-z]+".r
  val numbers = "[0-9]+".r
  val lettersOrNumbers = "[a-z0-9]+".r 
  
  test("matches") {
    assert(letters.foundIn("asdf") === true)
    assert(letters.foundIn("123") === false)
    assert(letters.foundIn("asdf123") === true)
    assert(letters.foundIn("123asdf") === true)
    
    assert(numbers.foundIn("asdf") === false)
    assert(numbers.foundIn("123") === true)
    assert(numbers.foundIn("asdf123") === true)
    assert(numbers.foundIn("123asdf") === true)
    
    assert(lettersOrNumbers.foundIn("asdf") === true)
    assert(lettersOrNumbers.foundIn("123") === true)
    assert(lettersOrNumbers.foundIn("asdf123") === true)
    assert(lettersOrNumbers.foundIn("123asdf") === true)
  }
  
  test("doesntMatch") {
    assert(letters.notFoundIn("asdf") === false)
    assert(letters.notFoundIn("123") === true)
    assert(letters.notFoundIn("asdf123") === false)
    assert(letters.notFoundIn("123asdf") === false)
    
    assert(numbers.notFoundIn("asdf") === true)
    assert(numbers.notFoundIn("123") === false)
    assert(numbers.notFoundIn("asdf123") === false)
    assert(numbers.notFoundIn("123asdf") === false)
    
    assert(lettersOrNumbers.notFoundIn("asdf") === false)
    assert(lettersOrNumbers.notFoundIn("123") === false)
    assert(lettersOrNumbers.notFoundIn("asdf123") === false)
    assert(lettersOrNumbers.notFoundIn("123asdf") === false)
  }
}
