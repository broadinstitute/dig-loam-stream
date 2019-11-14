package loamstream.loam

import java.nio.file.{ Files => JFiles }
import java.nio.file.Path

import scala.util.Success
import scala.util.Try

import loamstream.TestHelpers
import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamProject
import loamstream.model.jobs.Execution
import loamstream.model.jobs.LJob
import loamstream.util.Files
import loamstream.util.Loggable
import loamstream.util.Tries

/** Utils for testing specific LoamScripts */
object LoamScriptTestUtils extends Loggable {

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
    val engine = TestHelpers.loamEngine
    
    val results = run(engine, LoamProject(TestHelpers.config, scripts))
    
    assert(results.jobExecutionsOpt.isSuccess, results.compileResultOpt)
    assertOutputFilesExist(filePaths)
  }

  private final case class Result(
      compileResultOpt: Try[LoamCompiler.Result],
      jobExecutionsOpt: Try[Map[LJob, Execution]])
  
  private def run(engine: LoamEngine, project: LoamProject): Result = {
    info(s"Now compiling project with ${project.scripts.size} scripts.")
    
    val compileResults = engine.compile(project)
    
    compileResults match {
      case success @ LoamCompiler.Result.Success(_, _, graph) => {
        info(success.summary)
        val jobResults = engine.run(graph)
        Result(Success(success), Success(jobResults))
      }
      case _ => {
        error("Could not compile.")
        Result(Success(compileResults), Tries.failure("Could not compile"))
      }
    }
  }
}
