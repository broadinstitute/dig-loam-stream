package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths

import org.scalatest.FunSuite
import loamstream.model.jobs.{Execution, JobState, OutputRecord}
import loamstream.model.jobs.JobState.CommandResult
import loamstream.model.jobs.Output.PathOutput
import loamstream.util.Hashes
import loamstream.oracle.Resources.LocalResources


/**
 * @author clint
 * date: Aug 9, 2016
 */
final class SlickLoamDaoTest extends FunSuite with ProvidesSlickLoamDao {
  
  //scalastyle:off magic.number
  
  private val path0 = Paths.get("src/test/resources/for-hashing/foo.txt")
  private val path1 = Paths.get("src/test/resources/for-hashing/bigger")
  private val path2 = Paths.get("src/test/resources/for-hashing/empty.txt")
  private val path3 = Paths.get("src/test/resources/blarg")
  
  private lazy val hash0 = Hashes.sha1(path0)
  private lazy val hash1 = Hashes.sha1(path1)
  private lazy val hash2 = Hashes.sha1(path2)
  
  private def noOutputs: Boolean = dao.allOutputRecords.isEmpty
  
  private def noExecutions: Boolean = dao.allExecutions.isEmpty

  private def store(paths: Path*): Unit = {
    val outputs = paths.map { path =>
      val hash = Hashes.sha1(path)
      
      cachedOutput(path, hash)
    }
    
    val execution = Execution(JobState.CommandResult(0, Some(LocalResources)), outputs.toSet)
    
    dao.insertExecutions(execution)
  }
  
  private def storeFailures(paths: Path*): Unit = {
    val outputs = paths.map(OutputRecord(_))
    
    //NB: Failure
    val state = JobState.CommandResult(42, Some(LocalResources))
    
    assert(state.isFailure)
    
    val execution = Execution(state, outputs.toSet)
    
    dao.insertExecutions(execution)
  }

  test("insert/Read Outputs") {
    createTablesAndThen {
      assert(noOutputs)
      
      store(path0)
      
      val expected =  Seq(cachedOutput(path0, hash0))
      
      assert(dao.allOutputRecords === expected)
    }
  }

  test("insert/allOutputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      val expected = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1), cachedOutput(path2, hash2))

      //Use Sets to ignore order
      assert(dao.allOutputRecords.toSet == expected)
    }
  }
  
  test("insert/allOutputs - only failures") {
    createTablesAndThen {

      assert(noOutputs)

      storeFailures(path0, path1, path2)

      val expected = Set(OutputRecord(path0), OutputRecord(path1), OutputRecord(path2))

      //Use Sets to ignore order
      assert(dao.allOutputRecords.toSet == expected)
      assert(dao.allOutputRecords.forall(dao.findExecution(_).get.isFailure))
    }
  }
  
  test("insert/allOutputs - some failures") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1)
      storeFailures(path2, path3)

      val failedOutputs = Set(OutputRecord(path2), OutputRecord(path3))
      val hashedOutputs = Set(cachedOutput(path0, hash0), cachedOutput(path1, hash1))

      val all: Set[OutputRecord] = failedOutputs ++ hashedOutputs
      
      //Use Sets to ignore order
      assert(dao.allOutputRecords.toSet == all)
      assert(dao.allOutputRecords.count(dao.findExecution(_).get.isFailure) == failedOutputs.size)
    }
  }

  test("delete all Outputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      assert(dao.allOutputRecords.size == 3)

      dao.deletePathOutput(path0, path1, path2)

      assert(noOutputs)
    }
  }
  
  test("delete some Outputs") {
    createTablesAndThen {

      assert(noOutputs)

      store(path0, path1, path2)

      assert(dao.allOutputRecords.size == 3)

      dao.deletePathOutput(path0, path2)

      assert(dao.allOutputRecords.map(_.loc) == Seq(path1.toAbsolutePath.toString))
      
      dao.deletePathOutput(path0, path1)
      
      assert(noOutputs)
    }
  }
  
  test("insertExecutions/allExecutionRows") {
    createTablesAndThen {
      val output0 = cachedOutput(path0, hash0)
      val output1 = cachedOutput(path1, hash1)
      val output2 = cachedOutput(path2, hash2)
      
      val failed0 = Execution(CommandResult(42, Some(LocalResources)), Set(output0))
      val failed1 = Execution(CommandResult(1, Some(LocalResources)), Set.empty[OutputRecord])
      val succeeded = Execution(CommandResult(0, Some(LocalResources)), Set(output1, output2))

      assert(failed0.isFailure)
      assert(failed1.isFailure)
      assert(succeeded.isSuccess)
      
      assert(noOutputs)
      assert(noExecutions)

      dao.insertExecutions(failed0)
      val expected0 = Execution(CommandResult(42, Some(LocalResources)), OutputRecord(output0.loc))
      assert(dao.allExecutions.toSet === Set(expected0))
      
      dao.insertExecutions(succeeded, failed1)
      val expected1 = failed1
      val expected2 = succeeded
      assert(dao.allExecutions.toSet === Set(expected0, expected1, expected2))
    }
  }
  
  test("insertExecutions - CommandInvocationFailure") {
    createTablesAndThen {
      val output0 = PathOutput(path0)
      
      val failed = Execution(JobState.CommandInvocationFailure(new Exception), output0.toOutputRecord)

      assert(failed.isFailure)
      
      assert(noOutputs)
      assert(noExecutions)

      dao.insertExecutions(failed)
      
      val expected0 = Execution(CommandResult(-1, Some(LocalResources)), failedOutput(path0))
      
      assert(dao.allExecutions.toSet === Set(expected0))
    }
  }
  
  test("findExecution") {
    createTablesAndThen {
      val output0 = cachedOutput(path0, hash0)
      val output1 = cachedOutput(path1, hash1)
      val output2 = cachedOutput(path2, hash2)
      
      val ex0 = Execution(CommandResult(42, Some(LocalResources)), output0)
      val ex1 = Execution(CommandResult(0, Some(LocalResources)), output1, output2)
      val ex2 = Execution(CommandResult(1, Some(LocalResources)), Set.empty[OutputRecord])
      
      assert(ex0.isFailure)
      assert(ex1.isSuccess)
      assert(ex2.isFailure)
      
      assert(noOutputs)
      assert(noExecutions)

      dao.insertExecutions(ex0)
      
      val expected0 = Execution(CommandResult(42, Some(LocalResources)), failedOutput(path0))
      
      assert(dao.allExecutions.toSet === Set(expected0))
      
      assert(dao.findExecution(output0) === Some(expected0))

      assert(dao.findExecution(output1) === None)
      assert(dao.findExecution(output2) === None)
      
      dao.insertExecutions(ex1, ex2)
      
      val expected1 = ex1
      val expected2 = ex2
      
      assert(dao.allExecutions.toSet === Set(expected0, expected1, expected2))
      
      assert(dao.findExecution(output0) === Some(expected0))
      assert(dao.findExecution(output1) === Some(expected1))
      assert(dao.findExecution(output2) === Some(expected1))
    }
  }
  
  test("findOutput") {
    createTablesAndThen {
      assert(noOutputs)
      
      store(path0)
      
      assert(dao.findOutputRecord(PathOutput(path0).toOutputRecord) === Some(cachedOutput(path0, hash0)))
      
      assert(dao.findOutputRecord(path1) === None)
      
      storeFailures(path1)
      
      assert(dao.findOutputRecord(path0) === Some(cachedOutput(path0, hash0)))
      
      assert(dao.findOutputRecord(path1) === Some(failedOutput(path1)))
    }
  }
  //scalastyle:on magic.number
}
