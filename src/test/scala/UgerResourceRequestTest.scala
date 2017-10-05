import loamstream.TestHelpers
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 10/4/17
 */
final class UgerResourceRequestTest extends FunSuite {
  private val code = {
    s"""
       |val a = store[TXT].at("a.txt").asInput
       |val b = store[TXT].at("b.txt")
       |
       |uger(mem = 16, cores = 4, maxRuntime = 5) {
       |  cmd"cp $$a $$b".in(a).out(b)
       |}
    """.stripMargin
  }

  test("") {
    val executableOpt = TestHelpers.loamEngine.compileToExecutable(code)
    assert(executableOpt.nonEmpty)

    val jobs = executableOpt.get.jobs
    assert(jobs.size === 1)
  }
}
