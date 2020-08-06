package loamstream.drm.uger

import org.scalatest.FunSuite
import scala.util.Success
import loamstream.drm.DrmTaskId
import loamstream.drm.DrmStatus
import java.time.LocalDateTime
import loamstream.util.CommandInvoker
import loamstream.util.RunResults
import loamstream.util.Observables
import loamstream.TestHelpers
import loamstream.drm.DrmStatus.CommandResult

/**
 * @author clint
 * Jul 24, 2020
 */
final class QstatQacctPollerTest extends FunSuite {
  import QstatQacctPoller._
  
  // scalastyle:off line.size.limit
  private val headerLines = Seq(
    "job-ID     prior   name       user         state submit/start at     queue                          jclass                         slots ja-task-ID",
    "------------------------------------------------------------------------------------------------------------------------------------------------")
  
  private val dataLines = Seq(
    "19115592 0.56956 test.sh    cgilbert     r     07/24/2020 11:51:17 broad@uger-c104.broadinstitute                                    1 2",
    "19115592 0.56956 test.sh    cgilbert     r     07/24/2020 11:51:18 broad@uger-c104.broadinstitute                                    1 1")
    
  private val dataLinesWithBadStatuses = Seq(
    "19115592 0.56956 test.sh    cgilbert     xyz     07/24/2020 11:51:17 broad@uger-c104.broadinstitute                                    1 1",
    "19115592 0.56956 test.sh    cgilbert     lala     07/24/2020 11:51:18 broad@uger-c104.broadinstitute                                    1 2")
  // scalastyle:on line.size.limit
  
  val lines = headerLines ++ dataLines
    
  test("parseMultiTaskQacctResults") {
    val jobNumber = "2314325"
    
    val lines = {
      Seq("=================") ++
      QacctTestHelpers.actualQacctOutput(
          None, 
          None, 
          LocalDateTime.now, 
          LocalDateTime.now, 
          jobNumber = jobNumber,
          taskIndex = 3,
          exitCode = 42) ++
      Seq("=================") ++
      QacctTestHelpers.actualQacctOutput(
          None, 
          None, 
          LocalDateTime.now, 
          LocalDateTime.now, 
          jobNumber = jobNumber,
          taskIndex = 99)  ++
      Seq("=================") ++
      QacctTestHelpers.actualQacctOutput(
          None, 
          None, 
          LocalDateTime.now, 
          LocalDateTime.now, 
          jobNumber = jobNumber,
          taskIndex = 4,
          exitCode = 0)
    }
    
    val tid3 = DrmTaskId(jobNumber, 3)
    val tid4 = DrmTaskId(jobNumber, 4)
    
    val idsToLookFor = Set(tid3, tid4)
    
    val actual = QstatQacctPoller.QacctSupport.parseMultiTaskQacctResults(idsToLookFor)(jobNumber -> lines)
    
    val expected = Map(tid3 -> CommandResult(42), tid4 -> CommandResult(0))
    
    assert(actual === expected)
  }
  
  test("poll - happy path") {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val qstatInvocationFn: CommandInvoker.InvocationFn[Unit] = { _ => 
      Success(RunResults.Successful("MOCK_QSTAT", lines, Nil))
    }
    
    val qstatInvoker: CommandInvoker.Async[Unit] = new CommandInvoker.Async.JustOnce("MOCK_QSTAT", qstatInvocationFn)
    
    val qacctInvocationFn: CommandInvoker.InvocationFn[String] = { jobNumber =>
      val lines = {
        Seq("=================") ++
        QacctTestHelpers.actualQacctOutput(
            None, 
            None, 
            LocalDateTime.now, 
            LocalDateTime.now, 
            jobNumber = jobNumber,
            taskIndex = 3) ++
        Seq("=================") ++
        QacctTestHelpers.actualQacctOutput(
            None, 
            None, 
            LocalDateTime.now, 
            LocalDateTime.now, 
            jobNumber = "82375682365872365",
            taskIndex = 99) ++
        Seq("=================") ++
        QacctTestHelpers.actualQacctOutput(
            None, 
            None, 
            LocalDateTime.now, 
            LocalDateTime.now, 
            jobNumber = jobNumber,
            taskIndex = 99) 
      }
      
      Success(RunResults.Successful("MOCK_QACCT", lines, Nil))
    }
    
    val qacctInvoker: CommandInvoker.Async[String] = new CommandInvoker.Async.JustOnce("MOCK_QACCT", qacctInvocationFn)
    
    val poller = new QstatQacctPoller(qstatInvoker, qacctInvoker)
    
    import Observables.Implicits._
    
    val runningTaskIds = Seq(DrmTaskId("19115592", 2), DrmTaskId("19115592", 1))
    
    val finishedTaskId = DrmTaskId("19115592", 3)
    
    {
      val results = TestHelpers.waitFor(poller.poll(runningTaskIds).toSeq.firstAsFuture)
      
      val expected = Seq(
          runningTaskIds(0) -> Success(DrmStatus.Running),
          runningTaskIds(1) -> Success(DrmStatus.Running))
          
      assert(results === expected)
    }
    
    {
      val results = TestHelpers.waitFor(poller.poll(runningTaskIds :+ finishedTaskId).toSeq.firstAsFuture)
      
      val expected = Seq(
          runningTaskIds(0) -> Success(DrmStatus.Running),
          runningTaskIds(1) -> Success(DrmStatus.Running),
          finishedTaskId -> Success(DrmStatus.CommandResult(0)))
          
      assert(results === expected)
    }
  }
  
  test("parseQstatOutput - happy path") {
    val expected = Iterable(
      Success(DrmTaskId("19115592", 2) -> DrmStatus.Running),
      Success(DrmTaskId("19115592", 1) -> DrmStatus.Running))
        
    assert(QstatSupport.parseQstatOutput(lines).toList === expected)
    
    assert(QstatSupport.parseQstatOutput(dataLines).toList === expected)
    
    assert(QstatSupport.parseQstatOutput(headerLines).isEmpty)
    
    assert(QstatSupport.parseQstatOutput(Nil).isEmpty)
  }
  
  test("parseQstatOutput - bad lines should be ignored") {
    val Seq(d0, d1) = dataLines
    
    val lines = headerLines ++ Seq(d0, "some bogus line lalala", d1)
    
    val expected = Iterable(
      Success(DrmTaskId("19115592", 2) -> DrmStatus.Running),
      Success(DrmTaskId("19115592", 1) -> DrmStatus.Running))
        
    assert(QstatSupport.parseQstatOutput(lines).toList === expected)
  }
  
  test("parseQstatOutput - bad statuses should be failures") {
    val lines = headerLines ++ dataLinesWithBadStatuses
    
    val actual = QstatSupport.parseQstatOutput(lines).toList
    
    assert(actual.forall(_.isFailure))
    
    assert(actual.size === 2)
  }
  
  test("getByTaskId - happy path") {
    val expected = Map(
      DrmTaskId("19115592", 2) -> Success(DrmStatus.Running),
      DrmTaskId("19115592", 1) -> Success(DrmStatus.Running))
        
    assert(QstatSupport.getByTaskId(lines) === expected)
    
    assert(QstatSupport.getByTaskId(dataLines) === expected)
    
    assert(QstatSupport.getByTaskId(headerLines) === Map.empty)
    
    assert(QstatSupport.getByTaskId(Nil) === Map.empty)
  }
  
  test("getByTaskId - bad lines should be ignored") {
    val Seq(d0, d1) = dataLines
    
    val lines = headerLines ++ Seq(d0, "some bogus line lalala", d1)
    
    val expected = Map(
      DrmTaskId("19115592", 1) -> Success(DrmStatus.Running),
      DrmTaskId("19115592", 2) -> Success(DrmStatus.Running))
        
    assert(QstatSupport.getByTaskId(lines) === expected)
  }
  
  test("getByTaskId - bad statuses should be failures") {
    val lines = headerLines ++ dataLinesWithBadStatuses
    
    val actual = QstatSupport.getByTaskId(lines)
    
    assert(actual === Map.empty)
  }
  
  test("toDrmStatus") {
    import QstatSupport.toDrmStatus
    import DrmStatus._
    
    //happy paths
    assert(toDrmStatus("E") === Success(Failed))
    assert(toDrmStatus("h") === Success(QueuedHeld))
    assert(toDrmStatus("r") === Success(Running))
    assert(toDrmStatus("R") === Success(Running))
    assert(toDrmStatus("s") === Success(Suspended))
    assert(toDrmStatus("S") === Success(Suspended))
    assert(toDrmStatus("N") === Success(Suspended))
    assert(toDrmStatus("w") === Success(Queued))
    assert(toDrmStatus("qw") === Success(Queued))
    assert(toDrmStatus("d") === Success(Undetermined))
    assert(toDrmStatus("P") === Success(Undetermined))
    assert(toDrmStatus("t") === Success(Undetermined))
    assert(toDrmStatus("T") === Success(Undetermined))
    
    //unmapped strings
    assert(toDrmStatus("X").isFailure)
    assert(toDrmStatus("b").isFailure)
    assert(toDrmStatus("Q").isFailure)
    
    assert(toDrmStatus("").isFailure)
    assert(toDrmStatus("    ").isFailure)
    assert(toDrmStatus("asdf").isFailure)
    assert(toDrmStatus("12345").isFailure)
  }
  
  private def outputForTasks(idsToExitCodes: (DrmTaskId, Int)*): Seq[String] = {
    idsToExitCodes.map { case (tid, ec) =>
      "============" +: QacctTestHelpers.actualQacctOutput(
          None, 
          None, 
          LocalDateTime.now, 
          LocalDateTime.now,
          jobNumber = tid.jobId,
          taskIndex = tid.taskIndex,
          exitCode = ec)
    }.flatten
  }
  
  test("parseQacctResults - happy path") {
    val jobId = "foo"
    
    val tid0 = DrmTaskId(jobId, 99)
    val tid1 = DrmTaskId(jobId, 0)
    val tid2 = DrmTaskId(jobId, 42)
    
    import QacctSupport.parseMultiTaskQacctResults
    
    val output = outputForTasks(tid0 -> 4, tid1 -> 0, tid2 -> 0)
    
    val expected = Map(
        tid0 -> DrmStatus.CommandResult(4),
        tid1 -> DrmStatus.CommandResult(0),
        tid2 -> DrmStatus.CommandResult(0))
    
    assert(parseMultiTaskQacctResults(Set(tid0, tid1, tid2))(jobId -> output) === expected)
  }
  
  test("parseQacctResults - more results than we're looking for") {
    val jobId = "foo"
    
    val tid0 = DrmTaskId(jobId, 99)
    val tid1 = DrmTaskId(jobId, 0)
    val tid2 = DrmTaskId(jobId, 42)
    
    import QacctSupport.parseMultiTaskQacctResults
    
    val output = outputForTasks(tid0 -> 4, tid1 -> 0, tid2 -> 0)
    
    val expected = Map(
        tid0 -> DrmStatus.CommandResult(4),
        tid1 -> DrmStatus.CommandResult(0))
    
    assert(parseMultiTaskQacctResults(Set(tid0, tid1))(jobId -> output) === expected)
  }

  test("parseQacctResults - bad input") {
    val jobId = "foo"
    
    val tid0 = DrmTaskId(jobId, 99)
    val tid1 = DrmTaskId(jobId, 0)
    val tid2 = DrmTaskId(jobId, 42)
    
    import QacctSupport.parseMultiTaskQacctResults
    
    val lines = outputForTasks(tid0 -> 4, tid1 -> 0, tid2 -> 0)
    
    val missingField = lines.filterNot(_.startsWith("exit_status"))
    
    val brokenField = lines.map(_.replaceAll("exit_status", "blerg"))
    
    assert(parseMultiTaskQacctResults(Set(tid0, tid1))(jobId -> missingField) === Map.empty) 
  
    assert(parseMultiTaskQacctResults(Set(tid0, tid1))(jobId -> brokenField) === Map.empty)
  }
  
  test("parseQacctResults - some good input, some bad") {
    val jobId = "foo"
    
    val tid0 = DrmTaskId(jobId, 99)
    val tid1 = DrmTaskId(jobId, 0)
    val tid2 = DrmTaskId(jobId, 42)
    
    import QacctSupport.parseMultiTaskQacctResults
    
    val goodLines = outputForTasks(tid0 -> 4, tid2 -> 0)
    
    val badLines = outputForTasks(tid1 -> 0).map(_.replaceAll("exit_status", "blerg"))
    
    val lines = goodLines ++ badLines
    
    val expected = Map(tid0 -> DrmStatus.CommandResult(4), tid2 -> DrmStatus.CommandResult(0))
    
    assert(parseMultiTaskQacctResults(Set(tid0, tid1, tid2))(jobId -> lines) === expected) 
  }
}
