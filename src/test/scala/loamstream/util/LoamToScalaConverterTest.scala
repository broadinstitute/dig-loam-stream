package loamstream.util

import java.nio.file.Path

import org.scalafmt.Scalafmt
import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.loam.LoamScript
import loamstream.util.code.ObjectId
import loamstream.util.code.PackageId
import loamstream.util.code.RootPackageId
import loamstream.loam.LoamLoamScript

/**
 * @author clint
 * Jan 6, 2017
 */
final class LoamToScalaConverterTest extends FunSuite {
  import LoamToScalaConverter._
  import loamstream.TestHelpers.path
  import Paths.Implicits._
  
  private def format(scalaCode: String): String = Scalafmt.format(scalaCode).get
  
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
  
  test("locateDirs - non-recursive") {
    {
      val root = path("src/test/loam")
      
      val dirs = locateDirs(root, recursive = false)
      
      val expected = Set(
        path("src/test/loam/subdir/"),
        path("src/test/loam/subdir2"))
        
      assert(dirs === expected)
    }
    
    {
      val root = path("src/test/loam/subdir")
      
      val dirs = locateDirs(root, recursive = false)
      
      val expected = Set.empty
        
      assert(dirs === Set.empty)
    }
  }
  
  test("locateDirs - recursive") {
    {
      val root = path("src/test/loam")
      
      val dirs = locateDirs(root, recursive = true)
      
      val expected = Set(
        path("src/test/loam/subdir/"),
        path("src/test/loam/subdir2"),
        path("src/test/loam/subdir2/subsubdir/"),
        path("src/test/loam/subdir2/subsubdir2/"))
        
      assert(dirs === expected)
    }
    
    {
      val root = path("src/test/loam/subdir")
      
      val dirs = locateDirs(root, recursive = true)
      
      val expected = Set.empty
        
      assert(dirs === Set.empty)
    } 
  }
  
  test("pathParts") {
    val relative = path("foo/bar/baz")
    
    assert(pathParts(relative) === Seq("foo", "bar", "baz"))
    
    val absolute = path("/foo/bar/baz")
    
    assert(pathParts(absolute) === Seq("foo", "bar", "baz"))
    
    val relativeSingle = path("foo")
    
    assert(pathParts(relativeSingle) === Seq("foo"))
    
    val absoluteSingle = path("/foo")
    
    assert(pathParts(absoluteSingle) === Seq("foo"))
    
    assert(pathParts(path(".")) === Nil)
    
    val empty = absolute.relativize(absolute)
    
    assert(empty.toString === "")
    
    assert(pathParts(empty) === Nil)
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
      a -> LoamFileInfo(path("a.loam"), LoamLoamScript("a", read(a), None)),
      b -> LoamFileInfo(path("b.loam"), LoamLoamScript("b", read(b), None)),
      c -> LoamFileInfo(path("c.loam"), LoamLoamScript("c", read(c), None)))
      
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
      a -> LoamFileInfo(path("a.loam"), LoamLoamScript("a", read(a), None)),
      b -> LoamFileInfo(path("b.loam"), LoamLoamScript("b", read(b), None)),
      c -> LoamFileInfo(path("c.loam"), LoamLoamScript("c", read(c), None)),
      x -> LoamFileInfo(path("subdir/x.loam"), LoamLoamScript("x", read(x), Some(subPackage))),
      y -> LoamFileInfo(path("subdir/y.loam"), LoamLoamScript("y", read(y), Some(subPackage))))
      
    assert(filesToInfos === expected)
  }
  
  test("makeProjectContextOwnerCode - default package") {
    val (name, packageId, objectId, code) = makeProjectContextOwnerCode(Nil)
    
    assert(packageId === LoamScript.scriptsPackage)
    
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
        |  val loamProjectContext : LoamProjectContext = LoamProjectContext.empty(???) //TODO
        |}
      """.stripMargin
    }
    
    assert(StringUtils.assimilateLineBreaks(code) === StringUtils.assimilateLineBreaks(expectedCode))
  }
  
  test("makeProjectContextOwnerCode - sub package") {
    val (name, packageId, objectId, code) = makeProjectContextOwnerCode(Seq("foo", "bar"))
    
    val expectedName = "LoamProjectContextOwner"
    
    assert(name === expectedName)
    
    val expectedPackageId = LoamScript.scriptsPackage.getPackage("foo").getPackage("bar")

    assert(packageId === expectedPackageId)
    
    val expectedObjectId = expectedPackageId.getObject(expectedName).getObject("loamProjectContext")
    
    assert(objectId === expectedObjectId)
    
    val expectedCode = {
      s"""package loamstream.loam.scripts.foo.bar
        |
        |import loamstream.loam.LoamProjectContext
        |
        |object LoamProjectContextOwner {
        |  val loamProjectContext : LoamProjectContext = LoamProjectContext.empty(???) //TODO
        |}
      """.stripMargin
    }

    assert(StringUtils.assimilateLineBreaks(code) === StringUtils.assimilateLineBreaks(expectedCode))
  }
  
  test("getRelativeScriptPackageDir") {
    assert(getRelativeScriptPackageDir(Nil) === path("loamstream/loam/scripts"))
    
    assert(getRelativeScriptPackageDir(Seq("foo")) === path("loamstream/loam/scripts/foo"))
    
    assert(getRelativeScriptPackageDir(Seq("foo", "bar", "baz")) === path("loamstream/loam/scripts/foo/bar/baz"))
  }
  
  test("makeScalaFile - root dir") {
    val outputDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val a = path("src/test/loam/a.loam")
    
    import Files.{ readFromAsUtf8 => read }
    
    val script = LoamLoamScript("a", read(a), None)
    
    val expectedScalaFile = outputDir / "loamstream" / "loam" / "scripts" / "a.scala"
    
    val actualScalaFile = makeScalaFile(outputDir, a, LoamFileInfo(path("a.loam"), script))
    
    assert(actualScalaFile === expectedScalaFile)
    
    def exists(p: Path) = p.toFile.exists
    
    assert(exists(expectedScalaFile))
    
    val (_, packageId, contextValId: ObjectId, _) = makeProjectContextOwnerCode(Nil)
    
    val expectedCode = format(script.asScalaCode)
    
    assert(read(expectedScalaFile) === expectedCode)
  }
  
  test("makeScalaFile - sub dir") {
    val outputDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val x = path("src/test/loam/subdir/x.loam")
    
    import Files.{ readFromAsUtf8 => read }
    
    val script = LoamLoamScript("x", read(x), Some(PackageId("subdir")))
    
    val expectedScalaFile = outputDir / "loamstream" / "loam" / "scripts" / "subdir" / "x.scala"
    
    val actualScalaFile = makeScalaFile(outputDir, x, LoamFileInfo(path("subdir/x.loam"), script))
    
    assert(actualScalaFile === expectedScalaFile)
    
    def exists(p: Path) = p.toFile.exists
    
    assert(exists(expectedScalaFile))
    
    val (_, packageId, contextValId: ObjectId, _) = makeProjectContextOwnerCode(Seq("subdir"))
    
    val expectedCode = format(script.asScalaCode)
    
    assert(read(expectedScalaFile) === expectedCode)
  }
  
  test("convert") {
    val inputDir = path("src/test/loam/subdir")
    val outputDir = TestHelpers.getWorkDir(getClass.getSimpleName)
    
    val xLoam = path("src/test/loam/subdir/x.loam")
    val yLoam = path("src/test/loam/subdir/y.loam")
    
    import Files.{ readFromAsUtf8 => read }
    
    val xScript = LoamLoamScript("x", read(xLoam), None)
    val yScript = LoamLoamScript("y", read(yLoam), None)
    
    val expectedScalaFileX = outputDir / "loamstream/loam/scripts/x.scala"
    val expectedScalaFileY = outputDir / "loamstream/loam/scripts/y.scala"
    val expectedProjectContextOwnerScalaFiles = {
      Set(outputDir / "loamstream/loam/scripts/LoamProjectContextOwner.scala")
    }
    
    val filesMade = convert(inputDir, outputDir)
    
    val (actualProjectContextOwnerFiles, actualScalaFilesMade) = {
      filesMade.map(_.toPath).partition(_.toString.endsWith("LoamProjectContextOwner.scala"))
    }
    
    assert(actualProjectContextOwnerFiles.toSet === expectedProjectContextOwnerScalaFiles)
    
    assert(actualScalaFilesMade.toSet === Set(expectedScalaFileX, expectedScalaFileY))
    
    def exists(p: Path) = p.toFile.exists
    
    assert(exists(expectedProjectContextOwnerScalaFiles.head))
    assert(exists(expectedScalaFileX))
    assert(exists(expectedScalaFileY))
    
    val (_, _, contextValId: ObjectId, contextOwnerCode) = makeProjectContextOwnerCode(Nil)
    
    val expectedCodeX = format(xScript.asScalaCode)
    val expectedCodeY = format(yScript.asScalaCode)
    
    assert(read(expectedScalaFileX) === expectedCodeX)
    assert(read(expectedScalaFileY) === expectedCodeY)
    
    val expectedProjectContextOwnerCode = format(contextOwnerCode)
    
    assert(read(expectedProjectContextOwnerScalaFiles.head) === expectedProjectContextOwnerCode)
  }
  
  test("containsLoamFiles") {
    assert(containsLoamFiles(path("src/test/loam/")) === true)
    assert(containsLoamFiles(path("src/test/loam/subdir")) === true)
    assert(containsLoamFiles(path("src/test/loam/subdir2")) === false)
    assert(containsLoamFiles(path("src/test/loam/subdir2/subsubdir")) === false)
  }
}
