package loamstream.drm

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.util.Files
import loamstream.util.LogContext
import breeze.numerics.exp

/**
  * @author clint
  * @date Sep 29, 2021
  */
final class RateLimitedPollerTest extends FunSuite {
  test("readExitCodeFromStatsFile") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir => 
      val statsFile = workDir.resolve("stats")

      Files.writeTo(statsFile)("""|ExitCode: 42
                                  |Memory: 4640888k
                                  |System: 121.30s
                                  |User: 638.37s
                                  |End: 2021-09-29T16:14:20""".stripMargin)
      
      val actual = RateLimitedPoller.readExitCodeFromStatsFile(statsFile)(LogContext.Noop)

      val expected = Some(DrmStatus.CommandResult(42))

      assert(actual === expected)
    }
  }

  test("readExitCodeFromExitCodeFile") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir => 
      val statsFile = workDir.resolve("stats")

      Files.writeTo(statsFile)("42")
      
      val actual = RateLimitedPoller.readExitCodeFromExitCodeFile(statsFile)(LogContext.Noop)

      val expected = Some(DrmStatus.CommandResult(42))

      assert(actual === expected)
    }
  }
}
