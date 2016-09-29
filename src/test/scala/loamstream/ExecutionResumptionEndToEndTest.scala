package loamstream

import java.nio.file.{Path, Paths}

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.{LoamCompiler, LoamEngine}
import loamstream.db.slick.{ProvidesSlickLoamDao, TestDbDescriptors}
import loamstream.model.execute.{JobFilter, RxExecuter}
import loamstream.model.jobs.LJob.{SimpleSuccess, SkippedSuccess}
import loamstream.model.jobs.commandline.CommandLineJob.CommandSuccess
import loamstream.util.{Hit, Loggable, Sequence, Shot}
import org.scalatest.{FunSuite, Matchers}

/**
  * @author kaan
  *         date: Sep 27, 2016
  */
final class ExecutionResumptionEndToEndTest extends FunSuite with ProvidesSlickLoamDao with Loggable {

  override val descriptor = TestDbDescriptors.inMemoryH2

  private val resumptiveExecuter = RxExecuter.defaultWith(new JobFilter.DbBackedJobFilter(dao))

  test("Jobs are skipped if their outputs were already produced by a previous run") {
    // scalastyle:off no.whitespace.before.left.bracket

    createTablesAndThen {
      import Matchers._

      val workDir = makeWorkDir()

      val workDirInLoam = workDir.toString.replace("\\", "/")

      val outMessageSink = LoggableOutMessageSink(this)

      val loamEngine = LoamEngine(LoamCompiler(outMessageSink), resumptiveExecuter, outMessageSink)

      /* Loam for the first run that mimics an incomplete pipeline run:
          val fileIn = store[String].from("src/test/resources/a.txt")
          val fileOut1 = store[String].to("$workDir/fileOut1.txt")
          cmd"cp $$fileIn $$fileOut1
       */
      val firstScript =
      s"""val fileIn = store[String].from("src/test/resources/a.txt")
                         val fileOut1 = store[String].to("$workDirInLoam/fileOut1.txt")
                         cmd"cp $$fileIn $$fileOut1""""

      val firstCompiled = loamEngine.run(firstScript)

      val firstResults = for {
        (job, result) <- firstCompiled.jobResultsOpt.get
      } yield result

      val Hit(firstResultValues) = Shot.sequence(firstResults.toVector)

      firstResultValues should have size 2
      firstResultValues(0) shouldBe a[CommandSuccess]
      firstResultValues(1) shouldBe a[SimpleSuccess]

      // Prevent jobs launched as part of the first run from being considered as part of the subsequent run's
      resumptiveExecuter.clearStates()

      /* Loam for the second run that mimics a run launched subsequently to an incomplete first run:
          val fileIn = store[String].from("src/test/resources/a.txt")
          val fileOut1 = store[String].to("$workDir/fileOut1.txt")
          val fileOut2 = store[String].to("$workDir/fileOut2.txt")
          cmd"cp $$fileIn $$fileOut1"
          cmd"cp $$fileOut1 $$fileOut2
       */
      val secondScript =
      s"""val fileIn = store[String].from("src/test/resources/a.txt")
                            val fileOut1 = store[String].to("$workDirInLoam/fileOut1.txt")
                            val fileOut2 = store[String].to("$workDirInLoam/fileOut2.txt")
                            cmd"cp $$fileIn $$fileOut1"
                            cmd"cp $$fileOut1 $$fileOut2""""

      val secondCompiled = loamEngine.run(secondScript)

      val secondResults = for {
        (job, result) <- secondCompiled.jobResultsOpt.get
      } yield result

      val Hit(secondResultValues) = Shot.sequence(secondResults.toVector)

      secondResultValues should have size 3
      secondResultValues(0) shouldBe a[SkippedSuccess]
      secondResultValues(1) shouldBe a[CommandSuccess]
      secondResultValues(2) shouldBe a[SimpleSuccess]

      // scalastyle:off no.whitespace.before.left.bracket
    }
  }

  private val sequence: Sequence[Int] = Sequence()

  private def makeWorkDir(): Path = {
    def exists(path: Path): Boolean = path.toFile.exists

    val suffixes = sequence.iterator

    val candidates = suffixes.map(i => Paths.get(s"target/resumptive-executer-test$i"))

    val result = candidates.dropWhile(exists).next()

    val asFile = result.toFile

    asFile.mkdir()

    assert(asFile.exists)

    result
  }
}
