package loamstream.loam

import loamstream.TestHelpers
import loamstream.util.Files
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 4/11/17
 */
final class LanguageSupportTest extends FunSuite {
  import TestHelpers.config

  private def emptyProjectContext = LoamProjectContext.empty(config)

  test("embedding of Python snippets") {
    import LanguageSupport.Python._

    implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

    val someVal1 = 123
    val someVal2 = "ABC"

    val tool = python"$someVal1 foo bar $someVal2 baz"

    val commandLine = LoamCmdTool.toString(scriptContext.projectContext.fileManager, tool.tokens)
    val pieces = commandLine.split(" ")
    val binary = pieces.head
    val file = pieces.last
    val scriptContent = Files.readFrom(file)

    val expectedBinary = "/path/to/python/binary"
    val expectedScriptContent = "123 foo bar ABC baz"

    assert(binary === expectedBinary)
    assert(scriptContent === expectedScriptContent)
  }

  test("embedding of R snippets") {
    import LanguageSupport.R._

    implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

    val someVal1 = 456
    val someVal2 = "DEF"

    val tool = r"$someVal1 foo bar $someVal2 baz"

    val commandLine = LoamCmdTool.toString(scriptContext.projectContext.fileManager, tool.tokens)
    val pieces = commandLine.split(" ")
    val binary = pieces.head
    val file = pieces.last
    val scriptContent = Files.readFrom(file)

    val expectedBinary = "/path/to/R/binary"
    val expectedScriptContent = "456 foo bar DEF baz"

    assert(binary === expectedBinary)
    assert(scriptContent === expectedScriptContent)
  }
}
