package loamstream.loam

import java.nio.file.{Path, Files => JFiles}

import loamstream.loam.LoamScriptTestUtils.FilePaths
import org.scalatest.FunSuite
import loamstream.util.code.SourceUtils.Implicits.AnyToStringLiteral

/** Testing Loam scripts that can handle multiple projects in different directories */
final class LoamMultiProjectTest extends FunSuite {

  private final class FilePathsLocal(val projectName: String) extends FilePaths {
    override val rootDir: Path = JFiles.createTempDirectory("LoamMultiProjectTest")

    val allProjectsDir: Path = rootDir.resolve("projects")
    val projectDir: Path = allProjectsDir.resolve(projectName)
    val dataDir: Path = projectDir.resolve("data")
    val analysisDir: Path = projectDir.resolve("analysis")
    val resultsDir: Path = projectDir.resolve("results")

    override val subDirs: Seq[Path] = Seq(allProjectsDir, projectDir, dataDir, analysisDir, resultsDir)

    override val inFileName: String = s"$projectName.vcf"

    override val inFilePath: Path = dataDir.resolve(inFileName)

    val analysisFileName: String = s"$projectName.analysis.txt"
    val analysisFilePath: Path = analysisDir.resolve(analysisFileName)
    val resultsFileName: String = s"$projectName.results.txt"
    val resultsFilePath: Path = resultsDir.resolve(resultsFileName)

    override val outFileNames: Seq[String] = Seq(analysisFileName, resultsFileName)
    override val outFileDirs: Seq[Path] = Seq(analysisDir, resultsDir)
    override val outFilePaths: Seq[Path] = Seq(analysisFilePath, resultsFilePath)
  }

  private def createFilePaths(projectName: String): FilePathsLocal = new FilePathsLocal(projectName)

  private def createScripts(projectName: String, filePaths: FilePathsLocal): Seq[LoamScript] = {
    val projectScript = LoamScript("project",
      s"""
         |val name = ${projectName.asStringLiteral}
      """.stripMargin)
    val pipelineScript = LoamScript("pipeline",
      s"""
         |import project.name
         |
         |inDir(${filePaths.rootDir.asStringLiteral}) {
         |  val inFile = store.at(s"projects/$$name/data/$$name.vcf").asInput
         |  val analysisFile = store.at(s"projects/$$name/analysis/$$name.analysis.txt")
         |  val resultsFile = store.at(s"projects/$$name/results/$$name.results.txt")
         |
         |  cmd"cp $$inFile $$analysisFile"
         |  cmd"cp $$analysisFile $$resultsFile"
         |}
      """.stripMargin)
    Seq(projectScript, pipelineScript)
  }

  private def testForProject(projectName: String): Unit = {
    val filePaths = createFilePaths(projectName)
    val scripts = createScripts(projectName, filePaths)
    LoamScriptTestUtils.testScripts(scripts, filePaths)
  }

  test("Test for project name 'CAMP'") {
    testForProject("CAMP")
  }

  test("Test for project name 'BioMe'") {
    testForProject("BioMe")
  }

}
