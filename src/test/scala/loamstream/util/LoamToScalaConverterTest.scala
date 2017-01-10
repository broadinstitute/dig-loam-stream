package loamstream.util

import org.scalatest.FunSuite
import java.nio.file.Paths
import loamstream.TestHelpers
import loamstream.loam.LoamScript
import java.nio.file.Path
import loamstream.util.code.RootPackageId
import loamstream.util.code.ObjectId
import scalariform.formatter.ScalaFormatter
import loamstream.util.code.PackageId

/**
 * @author clint
 * Jan 6, 2017
 */
final class LoamToScalaConverterTest extends FunSuite {
  import LoamToScalaConverter._
  
  import TestHelpers.path
  
  test("listLoamFiles - non-recursive") {
    val root = path("src/test/loam")
    
    val files = listLoamFiles(root, recursive = false)
    
    val expected = Set(
      path("src/test/loam/a.loam"),
      path("src/test/loam/b.loam"),
      path("src/test/loam/c.loam"))
      
    assert(files.toSet === expected) 
  }
  
  test("listLoamFiles - recursive") {
    val root = path("src/test/loam")
    
    val files = listLoamFiles(root, recursive = true)
    
    val expected = Set(
      path("src/test/loam/a.loam"),
      path("src/test/loam/b.loam"),
      path("src/test/loam/c.loam"),
      path("src/test/loam/subdir/x.loam"),
      path("src/test/loam/subdir/y.loam"))
      
    assert(files.toSet === expected) 
  }
  
  test("locateAndParseLoamFiles - non-recursive") {
     val root = path("src/test/loam")
    
    val filesToInfos = locateAndParseLoamFiles(root, recursive = false)
    
    def parse(p: Path) = LoamScript.read(p, root)
    
    val srcTestLoam = path("src/test/loam/")
    
    import Files.{ readFromAsUtf8 => read }
    
    val a = path("src/test/loam/a.loam")
    val b = path("src/test/loam/b.loam")
    val c = path("src/test/loam/c.loam")
    
    val expected = Map(
      a -> LoamFileInfo(path("a.loam"), LoamScript("a", read(a), None)),
      b -> LoamFileInfo(path("b.loam"), LoamScript("b", read(b), None)),
      c -> LoamFileInfo(path("c.loam"), LoamScript("c", read(c), None)))
      
    assert(filesToInfos === expected)
  }
  
  test("locateAndParseLoamFiles - recursive") {
     val root = path("src/test/loam")
    
    val filesToInfos = locateAndParseLoamFiles(root, recursive = true)
    
    def parse(p: Path) = LoamScript.read(p, root)
    
    val srcTestLoam = path("src/test/loam/")
    
    import Files.{ readFromAsUtf8 => read }
    
    val a = path("src/test/loam/a.loam")
    val b = path("src/test/loam/b.loam")
    val c = path("src/test/loam/c.loam")
    val x = path("src/test/loam/subdir/x.loam")
    val y = path("src/test/loam/subdir/y.loam")
    
    val subPackage = RootPackageId.getPackage("subdir")
    
    val expected = Map(
      a -> LoamFileInfo(path("a.loam"), LoamScript("a", read(a), None)),
      b -> LoamFileInfo(path("b.loam"), LoamScript("b", read(b), None)),
      c -> LoamFileInfo(path("c.loam"), LoamScript("c", read(c), None)),
      x -> LoamFileInfo(path("subdir/x.loam"), LoamScript("x", read(x), Some(subPackage))),
      y -> LoamFileInfo(path("subdir/y.loam"), LoamScript("y", read(y), Some(subPackage))))
      
    assert(filesToInfos === expected)
  }
  
  test("makeProjectContextOwnerCode") {
    val (name, objectId, code) = makeProjectContextOwnerCode
    
    val expectedName = "LoamProjectContextOwner"
    
    assert(name === expectedName)
    
    val expectedObjectId = LoamScript.scriptsPackage.getObject(expectedName).getObject("loamProjectContext")
    
    assert(objectId === expectedObjectId)
    
    val expectedCode = {
      s"""package loamstream.loam.scripts
        |
        |import loamstream.loam.LoamProjectContext
        |
        |object LoamProjectContextOwner {
        |  val loamProjectContext : LoamProjectContext = LoamProjectContext.empty
        |}
      """.stripMargin
    }
    
    assert(code === expectedCode)
  }
  
  test("getRelativeScriptPackageDir") {
    assert(getRelativeScriptPackageDir === path("loamstream/loam/scripts"))
  }
  
  test("makeScalaFile - root dir") {
    val outputDir = path("target")
    
    val a = path("src/test/loam/a.loam")
    
    import Files.{ readFromAsUtf8 => read }
    
    val script = LoamScript("a", read(a), None)
    
    makeScalaFile(outputDir, a, LoamFileInfo(path("a.loam"), script))
    
    def exists(p: Path) = p.toFile.exists
    
    val expectedScalaFile = path("target/loamstream/loam/scripts/a.scala")
    
    assert(exists(expectedScalaFile))
    
    val (_, contextValId: ObjectId, _) = makeProjectContextOwnerCode
    
    val expectedCode = ScalaFormatter.format(script.asScalaCode(contextValId))
    
    assert(read(expectedScalaFile) === expectedCode)
  }
  
  test("makeScalaFile - sub dir") {
    val outputDir = path("target")
    
    val x = path("src/test/loam/subdir/x.loam")
    
    import Files.{ readFromAsUtf8 => read }
    
    val script = LoamScript("x", read(x), Some(PackageId("subdir")))
    
    makeScalaFile(outputDir, x, LoamFileInfo(path("subdir/x.loam"), script))
    
    def exists(p: Path) = p.toFile.exists
    
    val expectedScalaFile = path("target/loamstream/loam/scripts/subdir/x.scala")
    
    assert(exists(expectedScalaFile))
    
    val (_, contextValId: ObjectId, _) = makeProjectContextOwnerCode
    
    val expectedCode = ScalaFormatter.format(script.asScalaCode(contextValId))
    
    assert(read(expectedScalaFile) === expectedCode)
  }
}
