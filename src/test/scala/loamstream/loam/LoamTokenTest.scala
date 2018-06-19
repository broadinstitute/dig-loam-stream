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
import loamstream.model.execute.Locations
import java.nio.file.Path
import loamstream.util.Sequence


/**
 * @author clint
 * date: Jul 20, 2016
 */
final class LoamTokenTest extends FunSuite {

  import TestHelpers.path
  
  private def literalLocations[A](inContainerValue: => A, inHostValue: => A): Locations[A] = new Locations[A] {
    override def inHost(ignored: A): A = inHostValue
  
    override def inContainer(ignored: A): A = inContainerValue
  }
  
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
    def doTest(locations: Locations[Path]): Unit = TestHelpers.withScriptContext { implicit context =>
      import LoamToken.mergeStringTokens
      import LoamPredef._

      val storeA = store
      val storeB = store
  
      val storeRefToken = StoreRefToken(LoamStoreRef(storeA, identity), locations)
  
      val tokens = Seq(
          StringToken(""),
          StringToken("foo"),
          StringToken(" "),
          StoreToken(storeA, locations),
          StringToken("bar"),
          StoreToken(storeB, locations),
          storeRefToken,
          StringToken(""),
          StringToken(""),
          StringToken(" "),
          StringToken("baz"),
          StringToken(""),
          MultiToken(Seq(1,2,3)),
          MultiStoreToken(Seq(storeA, storeB), locations))
  
      val expected = Seq(
          StringToken("foo "),
          StoreToken(storeA, locations),
          StringToken("bar"),
          StoreToken(storeB, locations),
          storeRefToken,
          StringToken(" baz"),
          MultiToken(Seq(1,2,3)),
          MultiStoreToken(Seq(storeA, storeB), locations))
  
      assert(mergeStringTokens(tokens) == expected)
    }
    
    doTest(Locations.identity)
    doTest(literalLocations(path("foo"), path("bar")))
  }

  test("StringToken") {
    val s = "asdasdasfasf"

    val sPlus42 = "asdasdasfasf42"

    assert(StringToken(s).toString === s)
    assert(StringToken(s).toString === s)
    assert(StringToken(s) + StringToken("42") === StringToken(sPlus42))
  }

  test("StoreToken") {
    def doTest(locations: Locations[Path]): Unit = TestHelpers.withScriptContext { implicit context =>
      import LoamPredef._

      val pathStore = store("/foo/bar.txt")
      
      val u = uri("gs://loamstream/foo/bar")
      
      val uriStore = store(u)

      assert(StoreToken(pathStore, locations).toString === pathStore.toString)
      assert(StoreToken(uriStore, locations).toString === uriStore.toString)

      import BashScript.Implicits._
      
      assert(StoreToken(pathStore, locations).render === locations.inContainer(pathStore.path).render)
      assert(StoreToken(uriStore, locations).render === uriStore.render)
    }
    
    doTest(Locations.identity)
    doTest(literalLocations(path("foo"), path("bar")))
  }

  test("StoreRefToken") {
    def doTest(locations: Locations[Path], expected: String): Unit = TestHelpers.withScriptContext { implicit ctx =>
      import LoamPredef._

      val underlyingStore = store("foo.txt")
  
      val ref: LoamStoreRef =  underlyingStore + ".bar"
  
      import BashScript.Implicits._
      
      assert(ref.path.render == Paths.get("./foo.txt.bar").render)
  
      val refString = StoreRefToken(ref, locations).toString
      val renderedString = ref.render
      
      assert(refString == renderedString)
  
      assert(StoreRefToken(ref, locations).render === expected)
    }
    
    doTest(Locations.identity, "./foo.txt.bar")
    doTest(literalLocations(path("foo"), path("bar")), "foo")
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
    def doTest(locations: Locations[Path], expected: String): Unit = TestHelpers.withScriptContext { implicit ctx =>
      import LoamPredef._

      val txtStore = store("X.txt")
      val vcfStore = store("Y.txt")
  
      val multiStoreToken = MultiStoreToken(Seq(txtStore, vcfStore), locations)
      
      assert(multiStoreToken.toString === expected)
  
      assert(multiStoreToken.render === expected)
    }
    
    doTest(Locations.identity, "./X.txt ./Y.txt")
    
    doTest(literalLocations(path("foo"), ???), "foo foo")
  }
}
