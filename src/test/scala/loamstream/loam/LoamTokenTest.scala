package loamstream.loam

import org.scalatest.FunSuite
import loamstream.loam.LoamToken.StringToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.EnvToken

/**
 * @author clint
 * date: Jul 20, 2016
 */
final class LoamTokenTest extends FunSuite {
  test("mergeStringTokens - all StringTokens") {
    import LoamToken.mergeStringTokens
    
    val allStringTokens = Seq(
        StringToken(""), 
        StringToken("foo"), 
        StringToken(" "), 
        StringToken("bar"), 
        StringToken(""), 
        StringToken(""), 
        StringToken(" "), 
        StringToken("baz"), 
        StringToken(""))
        
    assert(mergeStringTokens(allStringTokens) == Seq(StringToken("foo bar baz")))
  }


  test("mergeStringTokens - mixed") {
    import LoamToken.mergeStringTokens
    // scalastyle:off null
    val tokens = Seq(
        StringToken(""), 
        StringToken("foo"), 
        StringToken(" "), 
        StoreToken(null),
        StringToken("bar"),
        StoreToken(null),
        StringToken(""), 
        StringToken(""), 
        StringToken(" "),
        EnvToken(null),
        StringToken("baz"), 
        StringToken(""))
        
    val expected = Seq(
        StringToken("foo "), 
        StoreToken(null), 
        StringToken("bar"), 
        StoreToken(null), 
        StringToken(" "), 
        EnvToken(null), 
        StringToken("baz"))
    // scalastyle:on null
    assert(mergeStringTokens(tokens) == expected)
  }
}