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
import loamstream.util.Traversables
import scala.util.Try
import scala.util.Failure
import loamstream.util.LogContext
import monix.execution.Scheduler
import loamstream.TestHelpers.DummyDrmJobOracle
import loamstream.util.Files
import scala.collection.compat._

/**
 * @author clint
 * Jul 24, 2020
 */
final class QstatPollerTest extends FunSuite {
  import QstatPoller._
  
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
  
  private val qstatLines = headerLines ++ dataLines
    
  test("poll - happy path") {
    val qstatInvocationFn: CommandInvoker.InvocationFn[Unit] = { _ => 
      Success(RunResults.Successful("MOCK_QSTAT", qstatLines, Nil))
    }
    
    import LogContext.Implicits.Noop
    import Scheduler.Implicits.global
    
    val qstatInvoker: CommandInvoker.Async[Unit] = {
      new CommandInvoker.Async.JustOnce("MOCK_QSTAT", qstatInvocationFn)
    }
    
    val poller = new QstatPoller(qstatInvoker)
    
    import Observables.Implicits._
    
    val runningTaskIds = Seq(DrmTaskId("19115592", 2), DrmTaskId("19115592", 1))
    
    val finishedTaskId = DrmTaskId("19115592", 3)
    
    //NB: Make sure finished job has an exit code recorded where we expect to find it.
    {
      val jobDir = DummyDrmJobOracle.dirFor(finishedTaskId)
      
      val exitcodeFile = jobDir.resolve("exitcode")
    
      jobDir.toFile.mkdirs()
      
      import java.nio.file.Files.exists
      
      assert(exists(jobDir))
      
      Files.writeTo(exitcodeFile)("0")
      
      assert(exists(exitcodeFile))
    }
    
    {

      val results = poller.poll(DummyDrmJobOracle)(runningTaskIds).toListL.runSyncUnsafe(TestHelpers.defaultWaitTime)
      
      val expected = Seq(
          runningTaskIds(0) -> Success(DrmStatus.Running),
          runningTaskIds(1) -> Success(DrmStatus.Running))
          
      assert(results === expected)
    }
    
    {
      val results = poller.poll(DummyDrmJobOracle)(runningTaskIds :+ finishedTaskId)
                          .toListL
                          .runSyncUnsafe(TestHelpers.defaultWaitTime)
      
      val expected = Seq(
          runningTaskIds(0) -> Success(DrmStatus.Running),
          runningTaskIds(1) -> Success(DrmStatus.Running),
          finishedTaskId -> Success(DrmStatus.CommandResult(0)))
          
      assert(results === expected)
    }
  }
  
  test("parseQstatOutput - happy path") {
    val id1 = DrmTaskId("19115592", 1)
    val id2 = DrmTaskId("19115592", 2)
    val id3 = DrmTaskId("19115592", 3)
    
    val ids = Set(id1, id2, id3)
    
    val expected = Iterable(
      Success(id2 -> DrmStatus.Running),
      Success(id1 -> DrmStatus.Running))
        
    assert(QstatSupport.parseQstatOutput(ids, qstatLines).toList === expected)
    
    assert(QstatSupport.parseQstatOutput(ids, dataLines).toList === expected)
    
    assert(QstatSupport.parseQstatOutput(ids, headerLines).isEmpty)
    
    assert(QstatSupport.parseQstatOutput(ids, Nil).isEmpty)
  }
  
  test("parseQstatOutput - happy path, some listings for whole task array") {
    val id1 = DrmTaskId("19115592", 1)
    val id2 = DrmTaskId("19115592", 2)
    val id3 = DrmTaskId("19115592", 3)
    
    val taskArrayId1 = "19115593"
    val taskArrayId2 = "19115594"
    
    val id11 = DrmTaskId(taskArrayId1,1)
    val id12 = DrmTaskId(taskArrayId1,2)
    
    val id22 = DrmTaskId(taskArrayId2,2)
    val id24 = DrmTaskId(taskArrayId2,4)
    val id26 = DrmTaskId(taskArrayId2,6)
    
    val ids = Set(id1, id2, id3, id11, id12, id22, id24, id26)
    
    //NB: convert to a map to ignore ordering
    val expected: Iterable[Try[(DrmTaskId, DrmStatus)]] = Seq(
      id1 -> DrmStatus.Running,
      id2 -> DrmStatus.Running,
      id11 -> DrmStatus.Queued,
      id12 -> DrmStatus.Queued,
      id22 -> DrmStatus.Running,
      id24 -> DrmStatus.Running,
      id26 -> DrmStatus.Running).map(Success(_))
    
    // scalastyle:off line.size.limit
    val lines = qstatLines ++ Seq(
        s"$taskArrayId2 0.56956 test.sh    cgilbert     r     07/24/2020 11:51:17 broad@uger-c104.broadinstitute                                   1 2-6:2",
        s"$taskArrayId1 0.56956 test.sh    cgilbert     qw     07/24/2020 11:51:18 broad@uger-c104.broadinstitute                                  1 1-2:1")
    // scalastyle:on line.size.limit
      
    def sort(s: Iterable[Try[(DrmTaskId, DrmStatus)]]): Iterable[Try[(DrmTaskId, DrmStatus)]] = {
      val ordering: Ordering[Try[(DrmTaskId, DrmStatus)]] = DrmTaskId.ordering.on {
        case Success((taskId, _)) => taskId
        case Failure(_) => ???
      }
      
      s.to(Seq).sorted(ordering)
    }
        
    assert(sort(QstatSupport.parseQstatOutput(ids, lines).toList) === sort(expected))
  }
  
  test("parseQstatOutput - bad lines should be ignored") {
    val Seq(d0, d1) = dataLines
    
    val lines = headerLines ++ Seq(d0, "some bogus line lalala", d1)
    
    val id1 = DrmTaskId("19115592", 1)
    val id2 = DrmTaskId("19115592", 2)
    val id3 = DrmTaskId("19115592", 3)
    
    val ids = Set(id1, id2, id3)
    
    val expected = Iterable(
      Success(id2 -> DrmStatus.Running),
      Success(id1 -> DrmStatus.Running))
        
    assert(QstatSupport.parseQstatOutput(ids, lines).toList === expected)
  }
  
  test("parseQstatOutput - bad statuses should be failures") {
    val lines = headerLines ++ dataLinesWithBadStatuses
    
    val id1 = DrmTaskId("19115592", 1)
    val id2 = DrmTaskId("19115592", 2)
    val id3 = DrmTaskId("19115592", 3)
    
    val ids = Set(id1, id2, id3)
    
    val actual = QstatSupport.parseQstatOutput(ids, lines).toList
    
    assert(actual.forall { 
      case Success((drmTaskId, drmStatus)) => drmStatus.isUndetermined
      case _ => false
    })
    
    assert(actual.size === 2)
  }
  
  test("getByTaskId - happy path") {
    val id1 = DrmTaskId("19115592", 1)
    val id2 = DrmTaskId("19115592", 2)
    val id3 = DrmTaskId("19115592", 3)
    
    val ids = Set(id1, id2, id3)
    
    val expected = Map(
      id2 -> Success(DrmStatus.Running),
      id1 -> Success(DrmStatus.Running))
        
    assert(QstatSupport.getByTaskId(ids, qstatLines) === expected)
    
    assert(QstatSupport.getByTaskId(ids, dataLines) === expected)
    
    assert(QstatSupport.getByTaskId(ids, headerLines) === Map.empty)
    
    assert(QstatSupport.getByTaskId(ids, Nil) === Map.empty)
  }
  
  test("getByTaskId - bad lines should be ignored") {
    val Seq(d0, d1) = dataLines
    
    val lines = headerLines ++ Seq(d0, "some bogus line lalala", d1)
    
    val id1 = DrmTaskId("19115592", 1)
    val id2 = DrmTaskId("19115592", 2)
    val id3 = DrmTaskId("19115592", 3)
    
    val ids = Set(id1, id2, id3)
    
    val expected = Map(
      id1 -> Success(DrmStatus.Running),
      id2 -> Success(DrmStatus.Running))
        
    assert(QstatSupport.getByTaskId(ids, lines) === expected)
  }
  
  test("getByTaskId - bad statuses should be failures") {
    val id1 = DrmTaskId("19115592", 1)
    val id2 = DrmTaskId("19115592", 2)
    val id3 = DrmTaskId("19115592", 3)
    
    val ids = Set(id1, id2, id3)
    
    val lines = headerLines ++ dataLinesWithBadStatuses
    
    val actual = QstatSupport.getByTaskId(ids, lines)
    
    assert(actual === Map.empty)
  }
  
  test("toDrmStatus") {
    import QstatSupport.toDrmStatus
    import DrmStatus._
    
    assert(toDrmStatus("qw") === Queued)
    assert(toDrmStatus("hqw") === Queued)
    assert(toDrmStatus("hRwq") === Queued)

    assert(toDrmStatus("r") === Running)
    assert(toDrmStatus("R") === Running)
    assert(toDrmStatus("t") === Running)
    assert(toDrmStatus("Rr") === Running)
    assert(toDrmStatus("Rt") === Running)

    assert(toDrmStatus("N") === Suspended)
    assert(toDrmStatus("s") === Suspended)
    assert(toDrmStatus("ts") === Suspended)
    assert(toDrmStatus("S") === Suspended)
    assert(toDrmStatus("tS") === Suspended)
    assert(toDrmStatus("T") === Suspended)
    assert(toDrmStatus("tT") === Suspended)
    assert(toDrmStatus("Rs") === Suspended)
    assert(toDrmStatus("Rts") === Suspended)
    assert(toDrmStatus("RS") === Suspended)
    assert(toDrmStatus("RtS") === Suspended)
    assert(toDrmStatus("RT") === Suspended)
    assert(toDrmStatus("RtT") === Suspended)

    assert(toDrmStatus("E") === Failed)
    assert(toDrmStatus("Eqw") === Failed)
    assert(toDrmStatus("Ehqw") === Failed)
    assert(toDrmStatus("EhRqw") === Failed)

    assert(toDrmStatus("d") === Failed)
    assert(toDrmStatus("dr") === Failed)
    assert(toDrmStatus("dt") === Failed)
    assert(toDrmStatus("dRr") === Failed)
    assert(toDrmStatus("dRt") === Failed)
    assert(toDrmStatus("ds") === Failed)
    assert(toDrmStatus("dS") === Failed)
    assert(toDrmStatus("dT") === Failed)
    assert(toDrmStatus("dRs") === Failed)
    assert(toDrmStatus("dRS") === Failed)
    assert(toDrmStatus("dRT") === Failed)
    
    //unmapped strings
    assert(toDrmStatus("X") === Undetermined)
    assert(toDrmStatus("b") === Undetermined)
    assert(toDrmStatus("Q") === Undetermined)
    
    assert(toDrmStatus("") === Undetermined)
    assert(toDrmStatus("    ") === Undetermined)
    assert(toDrmStatus("asdf") === Undetermined)
    assert(toDrmStatus("12345") === Undetermined)
  }
}
