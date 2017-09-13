package loamstream.loam

import loamstream.loam.LoamToken.{StoreToken, StoreRefToken, StringToken, MultiToken, MultiStoreToken}
import org.scalatest.FunSuite
import loamstream.loam.files.LoamFileManager
import loamstream.compiler.LoamPredef
import loamstream.conf.LoamConfig
import loamstream.TestHelpers
import java.nio.file.Paths

/**
 * @author clint
 * date: Jul 20, 2016
 */
final class LoamTokenTest extends FunSuite {
  //scalastyle:off null
  private def dummyFileManager: LoamFileManager = null.asInstanceOf[LoamFileManager]
  //scalastyle:on null
  
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
    import loamstream.loam.ops.StoreType.{TXT, VCF}
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)
    
    val storeA = store[TXT]
    val storeB = store[TXT]
    
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
    assert(StringToken(s).toString(dummyFileManager) === s)
    assert(StringToken(s) + StringToken("42") === StringToken(sPlus42))
  }
  
  test("StoreToken") {
    import LoamPredef._
    import loamstream.loam.ops.StoreType.{TXT, VCF}
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)

    val txtStore = store[TXT]
    val vcfStore = store[VCF]
    
    assert(StoreToken(txtStore).toString === txtStore.toString)
    
    val fooPath = Paths.get("foo.txt")
    
    val fileManager = LoamFileManager(Map(txtStore -> fooPath))
    
    assert(StoreToken(txtStore).toString(fileManager) === fooPath.toString)
  }
  
  test("StoreRefToken") {
    import LoamPredef._
    import loamstream.loam.ops.StoreType.{TXT, VCF}
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)
    
    val underlyingStore = store[TXT].at("foo.txt")
    
    val ref: LoamStoreRef =  underlyingStore + ".bar"
    
    assert(StoreRefToken(ref).toString === "./foo.txt.bar")
    
    val bazPath = Paths.get("baz")
    
    val fileManager = LoamFileManager(Map(underlyingStore -> bazPath))
    
    assert(StoreRefToken(ref).toString(fileManager) === "baz.bar")
  }
  
  test("MultiToken") {
    assert(MultiToken(Seq(9, 10, 11)).toString === "9 10 11")
    assert(MultiToken(Seq("x", "y", "z")).toString === "x y z")
    assert(MultiToken(Nil).toString === "")
    
    assert(MultiToken(Seq(9, 10, 11)).toString(dummyFileManager) === "9 10 11")
    assert(MultiToken(Seq("x", "y", "z")).toString(dummyFileManager) === "x y z")
    
    assert(MultiToken(Nil).toString(dummyFileManager) === "")
  }
  
  test("MultiStoreToken") {
    import LoamPredef._
    import loamstream.loam.ops.StoreType.{TXT, VCF}
    implicit val context = new LoamScriptContext(TestHelpers.emptyProjectContext)

    val txtStore = store[TXT]
    val vcfStore = store[VCF]
    
    assert(MultiStoreToken(Seq(txtStore, vcfStore)).toString === s"${txtStore.path} ${vcfStore.path}")

    val fooPath = Paths.get("foo.txt")
    val barPath = Paths.get("bar.vcf")
    
    val fileManager = LoamFileManager(Map(txtStore -> fooPath, vcfStore -> barPath))
    
    assert(MultiStoreToken(Seq(txtStore, vcfStore)).toString(fileManager) === s"$fooPath $barPath")
  }
}
