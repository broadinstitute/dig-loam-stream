package loamstream.loam

import java.nio.file.Paths

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.compiler.LoamPredef
import loamstream.conf.ExecutionConfig
import loamstream.loam.LoamToken.MultiStoreToken
import loamstream.loam.LoamToken.MultiToken
import loamstream.loam.LoamToken.StoreRefToken
import loamstream.loam.LoamToken.StoreToken
import loamstream.loam.LoamToken.StringToken
import loamstream.util.BashScript


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
    import LoamPredef._
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)

    val storeA = store
    val storeB = store

    val storeRefToken = StoreRefToken(LoamStoreRef(storeA, identity))

    val tokens = Seq(
        StringToken(""),
        StringToken("foo"),
        StringToken(" "),
        StoreToken(storeA),
        StringToken("bar"),
        StoreToken(storeB),
        storeRefToken,
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
        storeRefToken,
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

  test("StoreToken") {
    import LoamPredef._
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)

    val txtStore = store
    val vcfStore = store

    assert(StoreToken(txtStore).toString === txtStore.toString)

    import BashScript.Implicits._
    
    assert(StoreToken(txtStore).render === txtStore.path.render)
  }

  test("StoreRefToken") {
    import LoamPredef._
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)

    val underlyingStore = store("foo.txt")

    val ref: LoamStoreRef =  underlyingStore + ".bar"

    import BashScript.Implicits._
    
    assert(ref.path.render == Paths.get("./foo.txt.bar").render)

    val refString = StoreRefToken(ref).toString
    val renderedString = ref.render
    assert(refString == renderedString)

    assert(StoreRefToken(ref).render === "./foo.txt.bar")
  }

  test("MultiToken") {
    assert(MultiToken(Seq(9, 10, 11)).toString === "9 10 11")
    assert(MultiToken(Seq("x", "y", "z")).toString === "x y z")
    assert(MultiToken(Nil).toString === "")

    assert(MultiToken(Seq(9, 10, 11)).toString === "9 10 11")
    assert(MultiToken(Seq("x", "y", "z")).toString === "x y z")

    assert(MultiToken(Nil).toString === "")
  }

  test("MultiStoreToken") {
    import LoamPredef._
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)

    val txtStore = store
    val vcfStore = store

    val multiStoreToken = MultiStoreToken(Seq(txtStore, vcfStore))
    
    assert(multiStoreToken.toString === s"${txtStore.path} ${vcfStore.path}")

    assert(multiStoreToken.render === multiStoreToken.toString)
  }
}
