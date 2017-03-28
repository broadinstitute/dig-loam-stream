package loamstream.loam

import java.nio.file.{Path, Files => JFiles}

import loamstream.compiler.LoamEngine
import loamstream.util.Files
import loamstream.TestHelpers

/** Utils for testing specific LoamScripts */
object LoamScriptTestUtils {

  trait FilePaths {
    def rootDir: Path

    def rootDirs: Seq[Path] = Seq(rootDir)

    def subDirs: Seq[Path]

    def workDirs: Seq[Path] = rootDirs ++ subDirs

    def workDirsToPreCreate: Seq[Path] = workDirs

    def inFileName: String

    def inFilePath: Path = rootDir.resolve(inFileName)

    def outFileNames: Seq[String]

    def outFileDirs: Seq[Path]

    def outFilePaths: Seq[Path] = outFileDirs.zip(outFileNames).map({ case (dir, name) => dir.resolve(name) })
  }

  def createInputFiles(paths: FilePaths): Unit = {
    for (workDir <- paths.workDirsToPreCreate) {
      if (!JFiles.exists(workDir)) {
        JFiles.createDirectory(workDir)
      }
    }
    Files.writeTo(paths.inFilePath)("Yo!")
  }

  def assertOutputFileExists(path: Path): Unit =
    assert(JFiles.exists(path), s"Output file $path does not exist!")


  def assertOutputFilesExist(paths: FilePaths): Unit = {
    for (outFilePath <- paths.outFilePaths) {
      assertOutputFileExists(outFilePath)
    }
  }

  def testScript(script: LoamScript, filePaths: FilePaths): Unit = testScripts(Seq(script), filePaths)

  def testScripts(scripts: Iterable[LoamScript], filePaths: FilePaths): Unit = {
    createInputFiles(filePaths)
    val engine = LoamEngine.default(TestHelpers.config)
    val results = engine.run(scripts)
    assert(results.jobExecutionsOpt.nonEmpty, results.compileResultOpt)
    assertOutputFilesExist(filePaths)
  }

}
