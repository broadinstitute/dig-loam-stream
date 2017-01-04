package loamstream.model.execute

import java.nio.file.Paths
import java.time.Instant

import org.scalatest.{FunSuite, PrivateMethodTester}
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.{Execution, JobState, Output, OutputRecord}

/**
 * @author clint
 *         date: Sep 30, 2016
 */
final class DbBackedJobFilterTest extends FunSuite with ProvidesSlickLoamDao with PrivateMethodTester {
  //scalastyle:off magic.number

  private val p0 = Paths.get("src/test/resources/for-hashing/foo.txt")
  private val p1 = Paths.get("src/test/resources/for-hashing/empty.txt")
  private val p2 = Paths.get("src/test/resources/for-hashing/subdir/bar.txt")
  private val p3 = Paths.get("non/existent/blah.txt")

  private val o0 = Output.PathOutput(p0)
  private val o1 = Output.PathOutput(p1)
  private val o2 = Output.PathOutput(p2)
  private val o3 = Output.PathOutput(p3)

  private val cachedOutput0 = o0.toOutputRecord
  private val cachedOutput1 = o1.toOutputRecord
  private val cachedOutput2 = o2.toOutputRecord
  private val cachedOutput3 = o3.toOutputRecord

  private val failedOutput0 = failedOutput(p0)
  private val failedOutput1 = failedOutput(p1)
  private val failedOutput2 = failedOutput(p2)

  private def executions = dao.allExecutions.toSet

  import JobState._

  test("record() - no Executions") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      filter.record(Nil)

      assert(executions === Set.empty)
    }
  }

  test("record() - non-command-Execution") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val e = Execution(Succeeded)

      filter.record(Seq(e))

      assert(executions === Set.empty)
    }
  }

  test("record() - successful command-Execution, no outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(0)

      assert(cr.isSuccess)

      val e = Execution(cr)

      filter.record(Seq(e))

      assert(executions === Set(e))
    }
  }

  test("record() - failed command-Execution, no outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(42)

      assert(cr.isFailure)

      val e = Execution(cr)

      filter.record(Seq(e))

      assert(executions === Set(e))
    }
  }

  test("record() - successful command-Execution, some outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(0)

      assert(cr.isSuccess)

      val e = Execution.fromOutputs(cr, Set[Output](o0, o1, o2))
      val withHashedOutputs = e.withOutputRecords(Set(cachedOutput0, cachedOutput1, cachedOutput2))

      filter.record(Seq(e))

      assert(executions === Set(withHashedOutputs))
    }
  }

  test("record() - failed command-Execution, some outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      assert(executions === Set.empty)

      val cr = CommandResult(42)

      assert(cr.isFailure)

      val e = Execution.fromOutputs(cr, Set[Output](o0, o1, o2))

      filter.record(Seq(e))

      assert(executions === Set(Execution(CommandResult(42), failedOutput0, failedOutput1, failedOutput2)))
    }
  }

  test("needsToBeRun") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)

      // Expose a private method for testing purposes
      val needsToBeRun = PrivateMethod[Boolean]('needsToBeRun)

      assert(executions === Set.empty)

      val failure = CommandResult(42)
      assert(failure.isFailure)

      val success = CommandResult(0)
      assert(success.isSuccess)

      val failedExecs = Execution.fromOutputs(failure, Set[Output](o0))
      val successfulExecs = Execution.fromOutputs(success, Set[Output](o1, o3))

      filter.record(Seq(failedExecs, successfulExecs))

      // Missing record: 'needsToBeRun' --> true
      assert(cachedOutput3.isMissing)
      assert(filter invokePrivate needsToBeRun(cachedOutput3))

      // Older record: 'needsToBeRun' --> true
      val olderRec = cachedOutput1.withLastModified(Instant.ofEpochMilli(0))
      assert(filter invokePrivate needsToBeRun(olderRec))

      // Unhashed record: 'needsToBeRun' --> true
      assert(filter invokePrivate needsToBeRun(cachedOutput0))

      // Record with different hash: 'needsToBeRun' --> true
      val recWithDiffHash = OutputRecord(cachedOutput1.loc, Option("bogus-hash"), cachedOutput1.lastModified)
      assert(filter invokePrivate needsToBeRun(recWithDiffHash))

      // Otherwise: 'needsToBeRun' --> false
      assert(!(filter invokePrivate needsToBeRun(cachedOutput1)))
    }
  }

  test("hasDifferentHash") {

  }

  test("isOlder") {

  }

  //scalastyle:on magic.number
}
