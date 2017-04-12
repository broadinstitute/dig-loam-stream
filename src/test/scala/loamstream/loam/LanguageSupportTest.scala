package loamstream.loam

import loamstream.compiler.LoamPredef.store
import loamstream.TestHelpers
import loamstream.loam.ops.StoreType.TXT
import loamstream.util.Files
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 4/11/17
 */
final class LanguageSupportTest extends FunSuite {
  import TestHelpers.config

  private implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

  private def emptyProjectContext = LoamProjectContext.empty(config)

  private def doTest(loamLine: LoamCmdTool,
                     expectedBinary: String,
                     expectedScriptContent: String): Unit = {

    val commandLine = LoamCmdTool.toString(scriptContext.projectContext.fileManager, loamLine.tokens)
    val pieces = commandLine.split(" ")
    val binary = pieces.head
    val file = pieces.last
    val scriptContent = Files.readFrom(file)

    assert(binary === expectedBinary)
    assert(scriptContent === expectedScriptContent)
  }

  test("embedding of Python snippets") {
    import LanguageSupport.Python._

    val someTool = "someToolPath"
    val someVal = 123
    val someStore = store[TXT].at("/someStorePath")

    val loamLine = python"""$someTool --foo $someVal --bar $someStore baz"""

    val expectedBinary = "/path/to/python/binary"
    val expectedScriptContent = "someToolPath --foo 123 --bar /someStorePath baz"

    doTest(loamLine, expectedBinary, expectedScriptContent)
  }

  test("empty Python snippet") {
    import LanguageSupport.Python._

    val loamLine = python""
    val expectedBinary = "/path/to/python/binary"
    val expectedScriptContent = ""

    doTest(loamLine, expectedBinary, expectedScriptContent)
  }

  test("embedding of R snippets") {
    import LanguageSupport.R._

    val someTool = "someToooolPath"
    val someVal = 456
    val someStore = store[TXT].at("/someStooorePath")

    val loamLine = r"$someTool --foo $someVal --bar $someStore baz"

    val expectedBinary = "/path/to/R/binary"
    val expectedScriptContent = "someToooolPath --foo 456 --bar /someStooorePath baz"

    doTest(loamLine, expectedBinary, expectedScriptContent)
  }

  test("empty R snippet") {
    import LanguageSupport.R._

    val loamLine = r""""""
    val expectedBinary = "/path/to/R/binary"
    val expectedScriptContent = ""

    doTest(loamLine, expectedBinary, expectedScriptContent)
  }
}
