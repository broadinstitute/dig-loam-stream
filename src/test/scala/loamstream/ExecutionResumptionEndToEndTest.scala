package loamstream

import java.nio.file.{Path, Paths}

import loamstream.compiler.{LoamCompiler, LoamEngine}
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import org.scalatest.FunSuite
import loamstream.db.slick.TestDbDescriptors
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.execute.{JobFilter, RxExecuter}
import loamstream.model.jobs.commandline.CommandLineJob.CommandSuccess
import loamstream.util.{Hit, Loggable, Sequence}

/**
 * @author kaan
 * date: Sep 27, 2016
 */
final class ExecutionResumptionEndToEndTest extends FunSuite with ProvidesSlickLoamDao with Loggable {

  override val descriptor = TestDbDescriptors.inMemoryH2

  private val resumptiveExecuter = RxExecuter.defaultWith(new JobFilter.DbBackedJobFilter(dao))

  test("Jobs are skipped if their outputs were already produced by a previous run") {
    createTablesAndThen {
      val workDir = makeWorkDir()

      val outMessageSink = LoggableOutMessageSink(this)

      val loamEngine = LoamEngine(new LoamCompiler(outMessageSink), resumptiveExecuter, outMessageSink)

      /* Loam for the first run:

       */
      val firstScript =
        s"""val fileIn = store[String].from("src/test/resources/a.txt")
                         val fileOut1 = store[String].to("$workDir/fileOut1.txt")
                         cmd"cp $$fileIn $$fileOut1""""

      val firstCompiled = loamEngine.run(firstScript)

      for {
        (job, result) <- firstCompiled.jobResultsOpt.get
      } {
        debug(s"Got $result when running $job")
        //assert(result.get.isInstanceOf[CommandSuccess])
      }

      // Prevent jobs launched as part of the first run from being considered as part of the subsequent run's
      resumptiveExecuter.clearStates()

      /* Loam for the second run:

       */
      val secondScript =
        s"""val fileIn = store[String].from("src/test/resources/a.txt")
                            val fileOut1 = store[String].to("$workDir/fileOut1.txt")
                            val fileOut2 = store[String].to("$workDir/fileOut2.txt")
                            cmd"cp $$fileIn $$fileOut1"
                            cmd"cp $$fileOut1 $$fileOut2""""

      val secondCompiled = loamEngine.run(secondScript)

      for {
        (job, result) <- secondCompiled.jobResultsOpt.get
      } {
        debug(s"Got $result when running $job")
      }
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
