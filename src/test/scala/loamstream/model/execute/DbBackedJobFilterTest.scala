package loamstream.model.execute

import org.scalatest.FunSuite
import loamstream.db.slick.ProvidesSlickLoamDao
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobState
import loamstream.model.jobs.Output
import java.nio.file.Paths
import loamstream.model.jobs.Output.CachedOutput
import loamstream.db.slick.Helpers

/**
 * @author clint
 * date: Sep 30, 2016
 */
final class DbBackedJobFilterTest extends FunSuite with ProvidesSlickLoamDao {
  //TODO: MORE
  
  //scalastyle:off magic.number
  
  private val p0 = Paths.get("src/test/resources/for-hashing/foo.txt")
  private val p1 = Paths.get("src/test/resources/for-hashing/empty.txt")
  private val p2 = Paths.get("src/test/resources/for-hashing/subdir/bar.txt")
  
  private val o0 = Output.PathOutput(p0)
  private val o1 = Output.PathOutput(p1)
  private val o2 = Output.PathOutput(p2)
  
  private val cachedOutput0 = o0.toCachedOutput
  private val cachedOutput1 = o1.toCachedOutput
  private val cachedOutput2 = o2.toCachedOutput
  
  private def executions = dao.allExecutions.toSet
  
  import JobState._
  
  test("record() - no Executions") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
      
      def cache = filter.cache
      
      assert(executions === Set.empty)
      assert(cache === Map.empty)
      
      filter.record(Nil)
      
      assert(executions === Set.empty)
      assert(cache === Map.empty)
    }
  }
  
  test("record() - non-command-Execution") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
      
      def cache = filter.cache
      
      assert(executions === Set.empty)
      assert(cache === Map.empty)
      
      val e = Execution(Succeeded, Set.empty)
      
      filter.record(Seq(e))
      
      assert(executions === Set.empty)
      assert(cache === Map.empty)
    }
  }
  
  test("record() - successful command-Execution, no outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
      
      def cache = filter.cache
      
      assert(executions === Set.empty)
      assert(cache === Map.empty)
      
      val cr = CommandResult(0)
      
      assert(cr.isSuccess)
      
      val e = Execution(cr, Set.empty)
      
      filter.record(Seq(e))
      
      assert(executions === Set(e))
      assert(cache === Map.empty)
    }
  }
  
  test("record() - failed command-Execution, no outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
      
      def cache = filter.cache
      
      assert(executions === Set.empty)
      assert(cache === Map.empty)
      
      val cr = CommandResult(42)
      
      assert(cr.isFailure)
      
      val e = Execution(cr, Set.empty)
      
      filter.record(Seq(e))
      
      assert(executions === Set(e))
      assert(cache === Map.empty)
    }
  }
  
  test("record() - successful command-Execution, some outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
      
      def cache = filter.cache
      
      assert(executions === Set.empty)
      assert(cache === Map.empty)
      
      val cr = CommandResult(0)
      
      assert(cr.isSuccess)
      
      def normalizePath(co: CachedOutput): CachedOutput = {
        co.copy(path = Paths.get(Helpers.normalize(co.path)))
      }
      
      val normalized0 = normalizePath(cachedOutput0)
      val normalized1 = normalizePath(cachedOutput1)
      val normalized2 = normalizePath(cachedOutput2)
      
      val e = Execution(cr, Set(o0, o1, o2))
      val withHashedOutputs = e.withOutputs(Set(normalized0, normalized1, normalized2))
      
      filter.record(Seq(e))
      
      assert(executions === Set(withHashedOutputs))
      
      assert(cache === 
        Map(normalized0.path -> normalized0, normalized1.path -> normalized1, normalized2.path -> normalized2))
    }
  }
  
  test("record() - failed command-Execution, some outputs") {
    createTablesAndThen {
      val filter = new DbBackedJobFilter(dao)
      
      def cache = filter.cache
      
      assert(executions === Set.empty)
      assert(cache === Map.empty)
      
      val cr = CommandResult(42)
      
      assert(cr.isFailure)
      
      val e = Execution(cr, Set(o0, o1, o2))
      
      val withNoOutputs = e.withOutputs(Set.empty)
      
      filter.record(Seq(e))
      
      assert(executions === Set(withNoOutputs))
      assert(cache === Map.empty)
    }
  }
  
  //scalastyle:on magic.number
}