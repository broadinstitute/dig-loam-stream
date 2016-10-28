package loamstream.loam

import loamstream.loam.LoamScriptTestUtils.FilePaths
import org.scalatest.FunSuite

/** Testing Loam scripts that can handle multiple projects in different directories */
class LoamMultiProjectTest extends FunSuite {

  private def createFilePaths(projectName: String): FilePaths = ???

  private def createScripts(projectName: String, filePaths: FilePaths): Seq[LoamScript] = ???

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
