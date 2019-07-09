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
    assert(letters.matches("asdf") === true)
    assert(letters.matches("123") === false)
    assert(letters.matches("asdf123") === true)
    assert(letters.matches("123asdf") === true)
    
    assert(numbers.matches("asdf") === false)
    assert(numbers.matches("123") === true)
    assert(numbers.matches("asdf123") === true)
    assert(numbers.matches("123asdf") === true)
    
    assert(lettersOrNumbers.matches("asdf") === true)
    assert(lettersOrNumbers.matches("123") === true)
    assert(lettersOrNumbers.matches("asdf123") === true)
    assert(lettersOrNumbers.matches("123asdf") === true)
  }
  
  test("doesntMatch") {
    assert(letters.doesntMatch("asdf") === false)
    assert(letters.doesntMatch("123") === true)
    assert(letters.doesntMatch("asdf123") === false)
    assert(letters.doesntMatch("123asdf") === false)
    
    assert(numbers.doesntMatch("asdf") === true)
    assert(numbers.doesntMatch("123") === false)
    assert(numbers.doesntMatch("asdf123") === false)
    assert(numbers.doesntMatch("123asdf") === false)
    
    assert(lettersOrNumbers.doesntMatch("asdf") === false)
    assert(lettersOrNumbers.doesntMatch("123") === false)
    assert(lettersOrNumbers.doesntMatch("asdf123") === false)
    assert(lettersOrNumbers.doesntMatch("123asdf") === false)
  }
}
