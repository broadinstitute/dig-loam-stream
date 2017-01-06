package loamstream.util

import java.nio.file.{Path, Paths, Files => JFiles}

import org.scalatest.FunSuite

import scala.sys.process.ProcessLogger

/** Test of BashScript */
class BashScriptTest extends FunSuite {

  def pathToBashString(path: Path): String = BashScript.escapeString(path.toString)

  test("Create and run script") {
    val scriptPath = JFiles.createTempFile("bashScriptTestScript", ".sh")
    val inFilePath = JFiles.createTempFile("bashScriptTestInFile", ".txt")
    val outFilePath = JFiles.createTempFile("bashScriptTestInFile", ".txt")
    val inFilePathString = pathToBashString(inFilePath)
    val outFilePathString = pathToBashString(outFilePath)
    val scriptContent = s"cp $inFilePathString $outFilePathString"
    val script = BashScript.fromCommandLineString(scriptContent, scriptPath)
    assert(JFiles.exists(scriptPath), "Script file does not exist.")
    val currentDir = Paths.get(".")
    val processBuilder = script.processBuilder(currentDir)
    val noOpProcessLogger = ProcessLogger(line => ())
    Files.writeTo(inFilePath)("Yo!")
    val exitValue = processBuilder.run(noOpProcessLogger).exitValue()
    assert(exitValue === 0, "Script exit value is not 0.")
    assert(JFiles.exists(outFilePath), "Script output file does not exist.")
  }

  def testString(insert: String): String = s"So${insert}special!"

  test("BashScript.escapeString") {
    val specialChars: Set[Char] = Set('\\', '$', '\'', '\"', '\n', '\r', '\t', '\b', '\f', ' ')
    for (specialChar <- specialChars) {
      val string = testString(s"$specialChar")
      val expectedEscapedString = testString(s"\\$specialChar")
      val escapedString = BashScript.escapeString(string)
      assert(escapedString === expectedEscapedString)
    }
  }


}
