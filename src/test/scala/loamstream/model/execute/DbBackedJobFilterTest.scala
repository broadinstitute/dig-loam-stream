package loamstream.model.execute

import java.nio.file.Paths

import org.scalatest.FunSuite
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.{Execution, JobState, Output}

/**
 * @author clint
 * date: Sep 30, 2016
 */
final class DbBackedJobFilterTest extends FunSuite with ProvidesSlickLoamDao {
  //scalastyle:off magic.number
  
  private val p0 = Paths.get("src/test/resources/for-hashing/foo.txt")
  private val p1 = Paths.get("src/test/resources/for-hashing/empty.txt")
  private val p2 = Paths.get("src/test/resources/for-hashing/subdir/bar.txt")
  
  private val o0 = Output.PathOutput(p0)
  private val o1 = Output.PathOutput(p1)
  private val o2 = Output.PathOutput(p2)
  
  private val cachedOutput0 = o0.toOutputRecord
  private val cachedOutput1 = o1.toOutputRecord
  private val cachedOutput2 = o2.toOutputRecord
  
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
      
      assert(executions === Set(Execution(CommandResult(42), Set(cachedOutput0, cachedOutput1, cachedOutput2))))
    }
  }
  
  //scalastyle:on magic.number
}
