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
    
  test("poll - happy path") {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val qstatInvocationFn: CommandInvoker.InvocationFn[Unit] = { _ => 
      Success(RunResults.Successful("MOCK_QSTAT", lines, Nil))
    }
    
    val qstatInvoker: CommandInvoker[Unit] = new CommandInvoker.JustOnce("MOCK_QSTAT", qstatInvocationFn)
    
    val qacctInvocationFn: CommandInvoker.InvocationFn[DrmTaskId] = { tid =>
      val lines = {
        QacctTestHelpers.actualQacctOutput(
            None, 
            None, 
            LocalDateTime.now, 
            LocalDateTime.now, 
            jobNumber = tid.jobId,
            taskIndex = tid.taskIndex) 
      }
      
      Success(RunResults.Successful("MOCK_QACCT", lines, Nil))
    }
    
    val qacctInvoker: CommandInvoker[DrmTaskId] = new CommandInvoker.JustOnce("MOCK_QACCT", qacctInvocationFn)
    
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
  
  test("getExitStatus - happy path") {
    def outputWithExitCode(ec: Int) = {
      QacctTestHelpers.actualQacctOutput(None, None, LocalDateTime.now, LocalDateTime.now, ec)
    }
    
    assert(QacctSupport.getExitStatus(outputWithExitCode(0)) === Success(0))
    assert(QacctSupport.getExitStatus(outputWithExitCode(42)) === Success(42))
  }
  
  test("getExitStatus - bad input") {
    val lines = QacctTestHelpers.actualQacctOutput(None, None, LocalDateTime.now, LocalDateTime.now, 42)
    
    val missingField = lines.filterNot(_.startsWith("exit_status"))
    
    val brokenField = lines.map(_.replaceAll("exit_status", "blerg"))
    
    assert(QacctSupport.getExitStatus(missingField).isFailure)
    assert(QacctSupport.getExitStatus(brokenField).isFailure)
  }
  
  test("parseQacctResults - happy path") {
    def outputWithExitCode(ec: Int) = {
      QacctTestHelpers.actualQacctOutput(None, None, LocalDateTime.now, LocalDateTime.now, ec)
    }
    
    val tid = DrmTaskId("foo", 99)
    
    assert(QacctSupport.parseQacctResults(tid -> outputWithExitCode(0)) === (tid -> DrmStatus.CommandResult(0)))
    assert(QacctSupport.parseQacctResults(tid -> outputWithExitCode(42)) === (tid -> DrmStatus.CommandResult(42)))
  }
  
  test("parseQacctResults - bad input") {
    val lines = QacctTestHelpers.actualQacctOutput(None, None, LocalDateTime.now, LocalDateTime.now, 42)
    
    val missingField = lines.filterNot(_.startsWith("exit_status"))
    
    val brokenField = lines.map(_.replaceAll("exit_status", "blerg"))
    
    val tid = DrmTaskId("foo", 99)
    
    {
      val (actualTid, actualStatus) = QacctSupport.parseQacctResults(tid -> missingField) 
      
      assert(actualTid === tid)
      assert(actualStatus === DrmStatus.Undetermined)
    }
    
    {
      val (actualTid, actualStatus) = QacctSupport.parseQacctResults(tid -> brokenField) 
      
      assert(actualTid === tid)
      assert(actualStatus === DrmStatus.Undetermined)
    }
  }
}
