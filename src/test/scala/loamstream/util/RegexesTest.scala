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
  
  test("matchesAnywhere") {
    assert(letters.matchesAnywhere("asdf") === true)
    assert(letters.matchesAnywhere("123") === false)
    assert(letters.matchesAnywhere("asdf123") === true)
    assert(letters.matchesAnywhere("123asdf") === true)
    
    assert(numbers.matchesAnywhere("asdf") === false)
    assert(numbers.matchesAnywhere("123") === true)
    assert(numbers.matchesAnywhere("asdf123") === true)
    assert(numbers.matchesAnywhere("123asdf") === true)
    
    assert(lettersOrNumbers.matchesAnywhere("asdf") === true)
    assert(lettersOrNumbers.matchesAnywhere("123") === true)
    assert(lettersOrNumbers.matchesAnywhere("asdf123") === true)
    assert(lettersOrNumbers.matchesAnywhere("123asdf") === true)
  }
  
  test("doesntMatchAnywhere") {
    assert(letters.doesntMatchAnywhere("asdf") === false)
    assert(letters.doesntMatchAnywhere("123") === true)
    assert(letters.doesntMatchAnywhere("asdf123") === false)
    assert(letters.doesntMatchAnywhere("123asdf") === false)
    
    assert(numbers.doesntMatchAnywhere("asdf") === true)
    assert(numbers.doesntMatchAnywhere("123") === false)
    assert(numbers.doesntMatchAnywhere("asdf123") === false)
    assert(numbers.doesntMatchAnywhere("123asdf") === false)
    
    assert(lettersOrNumbers.doesntMatchAnywhere("asdf") === false)
    assert(lettersOrNumbers.doesntMatchAnywhere("123") === false)
    assert(lettersOrNumbers.doesntMatchAnywhere("asdf123") === false)
    assert(lettersOrNumbers.doesntMatchAnywhere("123asdf") === false)
  }
}
