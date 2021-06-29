package loamstream.drm

import org.scalatest.FunSuite
import java.time.LocalDateTime
import loamstream.TestHelpers
import loamstream.util.Files
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory

/**
  * @author clint
  * @date Jun 23, 2021
  *
  */
final class ExecutionStatsTest extends FunSuite {
  test("fromFile - happy path") {
    val start = LocalDateTime.now
    val end = LocalDateTime.now

    val lines = scala.util.Random.shuffle(Seq(
      " ExitCode 42",
      "Memory   1234k  ",
      "System\t4.56s",
      "User\t\t50s",
      s"Start ${start.toString}",
      s"End\t   ${end.toString}"))

    val expected = ExecutionStats(
      exitCode = 42,
      memory = Some(Memory.inKb(1234)),
      cpu = CpuTime.inSeconds(54.56),
      startTime = start,
      endTime = end,
      terminationReason = None)

    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("foo.stats")

      Files.writeLinesTo(file)(lines)

      assert(ExecutionStats.fromFile(file).get === expected)
    }
  }

  test("toDrmResources") {
    ???
  }
}
