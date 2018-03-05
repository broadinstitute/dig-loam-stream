package loamstream.loam

import java.nio.file.{Path, Files => JFiles}

import loamstream.compiler.LoamEngine
import loamstream.util.Files
import loamstream.TestHelpers
import loamstream.compiler.LoamProject
import loamstream.util.Loggable
import loamstream.util.Hit
import loamstream.util.Miss
import loamstream.compiler.LoamCompiler
import loamstream.util.Shot
import loamstream.model.jobs.LJob
import loamstream.model.jobs.Execution

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
    
    assert(results.jobExecutionsOpt.nonEmpty, results.compileResultOpt)
    assertOutputFilesExist(filePaths)
  }

  private final case class Result(
      compileResultOpt: Shot[LoamCompiler.Result],
      jobExecutionsOpt: Shot[Map[LJob, Execution]])
  
  private def run(engine: LoamEngine, project: LoamProject): Result = {
    info(s"Now compiling project with ${project.scripts.size} scripts.")
    
    val compileResults = engine.compile(project)
    
    if (compileResults.isValid) {
      info(compileResults.summary)
      //TODO: What if compileResults.contextOpt is None?
      val context = compileResults.contextOpt.get
      val jobResults = engine.run(context.graph)
      Result(Hit(compileResults), Hit(jobResults))
    } else {
      error("Could not compile.")
      Result(Hit(compileResults), Miss("Could not compile"))
    }
  }
}
