package loamstream.drm

import org.scalatest.FunSuite
import java.time.LocalDateTime
import loamstream.TestHelpers
import loamstream.util.Files
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.model.jobs.TerminationReason

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
    val start = LocalDateTime.now
    val end = LocalDateTime.now

    val stats = ExecutionStats(
      exitCode = 42,
      memory = Some(Memory.inKb(1234)),
      cpu = CpuTime.inSeconds(54.56),
      startTime = start,
      endTime = end,
      terminationReason = Some(TerminationReason.RunTime))

    def doTest(drmSystem: DrmSystem): Unit = {
      val actual = stats.toDrmResources(drmSystem)(
        node = Some("some-node"), 
        queue = Some(Queue("some-queue")),
        derivedFrom = Some("derived-from"))

      val expected = Some(drmSystem.resourcesMaker(
        Memory.inKb(1234),
        CpuTime.inSeconds(54.56),
        Some("some-node"),
        Some(Queue("some-queue")),
        start,
        end,
        Some("derived-from")))

      assert(actual === expected)
    }

    doTest(DrmSystem.Lsf)
    doTest(DrmSystem.Uger)
    doTest(DrmSystem.Slurm)
  }
}
