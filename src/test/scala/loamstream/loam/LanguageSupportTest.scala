package loamstream.loam

import loamstream.TestHelpers
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 4/11/17
 */
final class LanguageSupportTest extends FunSuite {
  import LoamCmdTool._
  import LanguageSupport.Python._
  import TestHelpers.config

  private def emptyProjectContext = LoamProjectContext.empty(config)

  test("string interpolation for Python snippets") {
    implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

    //val tool = python"foo bar baz"
  }
}
