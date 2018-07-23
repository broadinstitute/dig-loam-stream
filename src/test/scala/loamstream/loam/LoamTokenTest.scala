package loamstream.loam

import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.compiler.LoamPredef
import loamstream.conf.ExecutionConfig
import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.StringToken
import loamstream.util.BashScript
import java.nio.file.Path
import loamstream.util.Sequence
import loamstream.LoamFunSuite
import loamstream.LoamFunSuite


/**
 * @author clint
 * date: Jul 20, 2016
 */
final class LoamTokenTest extends LoamFunSuite {

  import TestHelpers.path
  
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

  testWithScriptContext("mergeStringTokens - mixed") { implicit context =>
    import LoamToken.mergeStringTokens
    import LoamPredef._

    val storeA = store
    val storeB = store

    val tokens = Seq(
        StringToken(""),
        StringToken("foo"),
        StringToken(" "),
        StoreToken(storeA),
        StringToken("bar"),
        StoreToken(storeB),
        StringToken(""),
        StringToken(""),
        StringToken(" "),
        StringToken("baz"),
        StringToken(""),
        MultiToken(Seq(1,2,3)),
        MultiStoreToken(Seq(storeA, storeB)))

    val expected = Seq(
        StringToken("foo "),
        StoreToken(storeA),
        StringToken("bar"),
        StoreToken(storeB),
        StringToken(" baz"),
        MultiToken(Seq(1,2,3)),
        MultiStoreToken(Seq(storeA, storeB)))

    assert(mergeStringTokens(tokens) == expected)
  }

  test("StringToken") {
    val s = "asdasdasfasf"

    val sPlus42 = "asdasdasfasf42"

    assert(StringToken(s).toString === s)
    assert(StringToken(s).toString === s)
    assert(StringToken(s) + StringToken("42") === StringToken(sPlus42))
  }

  testWithScriptContext("StoreToken") { implicit context =>
    import LoamPredef._

    val pathStore = store("/foo/bar.txt")
    
    val u = uri("gs://loamstream/foo/bar")
    
    val uriStore = store(u)

    assert(StoreToken(pathStore).toString === pathStore.toString)
    assert(StoreToken(uriStore).toString === uriStore.toString)

    import BashScript.Implicits._
    
    assert(StoreToken(pathStore).render === pathStore.path.render)
    assert(StoreToken(uriStore).render === uriStore.render)
  }

  test("MultiToken") {
    assert(MultiToken(Seq(9, 10, 11)).toString === "9 10 11")
    assert(MultiToken(Seq("x", "y", "z")).toString === "x y z")
    assert(MultiToken(Nil).toString === "")

    assert(MultiToken(Seq(9, 10, 11)).toString === "9 10 11")
    assert(MultiToken(Seq("x", "y", "z")).toString === "x y z")

    assert(MultiToken(Nil).toString === "")
  }

  testWithScriptContext("MultiStoreToken") { implicit ctx =>
    import LoamPredef._

    val txtStore = store("X.txt")
    val vcfStore = store("Y.txt")

    val multiStoreToken = MultiStoreToken(Seq(txtStore, vcfStore))
    
    assert(multiStoreToken.toString === "./X.txt ./Y.txt")

    assert(multiStoreToken.render === "./X.txt ./Y.txt")
  }
}
