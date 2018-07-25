package loamstream.loam

import java.nio.file.{ Files => JFiles }
import java.nio.file.Path

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.compiler.LoamPredef
import loamstream.loam.LoamScriptTestUtils.FilePaths
import loamstream.util.Paths


/**
  * LoamStream
  * Created by oliverr on 10/17/2016.
  */
final class LoamWorkDirTest extends FunSuite {

  private def assertPathsEquivalent(path1: Path, path2: Path): Unit = assert(path1.normalize() === path2.normalize())

  private def assertPathsEquivalent(path1: Path, path2: String): Unit = {
    assertPathsEquivalent(path1, TestHelpers.path(path2))
  }

  private def assertWorkDirIsSet(workDirOpt1: Option[Path], workDirOpt2: Option[Path]): Unit = {
    import LoamCmdTool._
    import loamstream.compiler.LoamPredef._
    implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty(TestHelpers.config))
    workDirOpt1 match {
      case Some(workDir1) => changeDir(workDir1)
      case None => ()
    }
    workDirOpt2 match {
      case Some(workDir2) => changeDir(workDir2)
      case None => ()
    }
    val fileName1 = "file1.vcf"
    val fileName2 = "file2.vcf"
    val store1 = store(fileName1).asInput
    val store2 = store(fileName2).asInput
    val tool = cmd"yo $store1 $store2"
    val workDir = (workDirOpt1, workDirOpt2) match {
      case (Some(workDir1), Some(workDir2)) => workDir1.resolve(workDir2)
      case (Some(workDir1), None) => workDir1
      case (None, Some(workDir2)) => workDir2
      case (None, None) => path(".")
    }
    assertPathsEquivalent(store1.path, workDir.resolve(fileName1))
    assertPathsEquivalent(store2.path, workDir.resolve(fileName2))
    assertPathsEquivalent(tool.workDirOpt.get, workDir)
  }

  test("Check work dirs and file paths are correctly set in graph") {
    import TestHelpers.path
    
    val workDirs = Seq(path("."), Paths.newRelative("a", "b", "c"), Paths.newAbsolute("a", "b", "c"))
    val workDirOpts = workDirs.map(Option(_)) :+ None
    for (workDirOpt1 <- workDirOpts) {
      for (workDirOpt2 <- workDirOpts) {
        assertWorkDirIsSet(workDirOpt1, workDirOpt2)
      }
    }
  }

  private class FilePathsLocal extends FilePaths {
    override val rootDirs: Seq[Path] = Seq.fill(2)(JFiles.createTempDirectory("LoamWorkDirTest"))
    override val rootDir: Path = rootDirs.head
    val subDirName = "subDir"
    override val subDirs: Seq[Path] = rootDirs.map(_.resolve(subDirName))
    override val workDirsToPreCreate: Seq[Path] = Seq.empty
    override val inFileName = "inFile.txt"
    override val outFileNames: Seq[String] = (0 to 5).map(index => s"outFile$index.txt")
    override val outFileDirs: Seq[Path] =
      Seq(rootDirs.head, subDirs.head, subDirs.head, rootDirs(1), subDirs(1), subDirs(1))
  }

  private def createFilePaths: FilePathsLocal = new FilePathsLocal

  private def createGraphUsingChangeDir(paths: FilePathsLocal): LoamGraph = TestHelpers.makeGraph { implicit sc =>
    import LoamPredef._
    import LoamCmdTool._
    
    changeDir(paths.rootDirs.head)
    val inFile = store(paths.inFileName).asInput
    val outFile0 = store(paths.outFileNames.head)
    cmd"cp $inFile $outFile0"
    changeDir(paths.subDirName)
    val outFile1 = store(paths.outFileNames(1))
    val outFile2 = store(paths.outFileNames(2))
    cmd"cp $outFile0 $outFile1"
    cmd"cp $outFile1 $outFile2"
    changeDir(paths.rootDirs(1))
    val outFile3 = store(paths.outFileNames(3))
    cmd"cp $outFile2 $outFile3"
    changeDir(paths.subDirName)
    val outFile4 = store(paths.outFileNames(4))
    val outFile5 = store(paths.outFileNames(5))
    cmd"cp $outFile3 $outFile4"
    cmd"cp $outFile4 $outFile5"
  }

  private def createGraphUsingInDir(paths: FilePathsLocal): LoamGraph = TestHelpers.makeGraph { implicit sc =>
    import LoamPredef._
    import LoamCmdTool._

    val outFile2 = store
    inDir(paths.rootDirs.head) {
      val inFile = store(paths.inFileName).asInput
      val outFile0 = store(paths.outFileNames.head)
      cmd"cp $inFile $outFile0"
      inDir(paths.subDirName) {
        val outFile1 = store(paths.outFileNames(1))
        val outFile2Prime = store(paths.outFileNames(2))
        cmd"cp $outFile0 $outFile1"
        cmd"cp $outFile1 $outFile2Prime"
      }
    }
    inDir(paths.rootDirs(1)) {
      val outFile3 = store(paths.outFileNames(3))
      cmd"cp $outFile2 $outFile3"
      inDir(paths.subDirName) {
        val outFile4 = store(paths.outFileNames(4))
        val outFile5 = store(paths.outFileNames(5))
        cmd"cp $outFile3 $outFile4"
        cmd"cp $outFile4 $outFile5"
      }
    }
  }

  private def doTest(graph: LoamGraph, filePaths: FilePathsLocal): Unit = {
    LoamScriptTestUtils.createInputFiles(filePaths)
    
    val results = TestHelpers.run(graph)
    
    assert(results.nonEmpty, "No jobs were executed")
    
    LoamScriptTestUtils.assertOutputFilesExist(filePaths)
  }
  
  test("Toy pipeline of cp using changeDir(Path)") {
    val filePaths = createFilePaths
    
    doTest(createGraphUsingChangeDir(filePaths), filePaths)
  }

  test("Toy pipeline of cp using inDir(Path) {...} ") {
    val filePaths = createFilePaths
    
    doTest(createGraphUsingInDir(filePaths), filePaths)
  }
}

