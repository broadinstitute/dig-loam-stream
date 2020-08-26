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
  
  private val qstatLines = headerLines ++ dataLines
    
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
    
    assert(actual.toMap === expected)
  }
  
  test("parseMultiTaskQacctResults - problematic qacct output") {
    val jobNumber = "19290502"
    
    val lines = problematicQacctOutput.trim.split("[\\r\\n]+")
    
    val tid = DrmTaskId(jobNumber, 1)
    
    val idsToLookFor = Set(tid)
    
    val actual = QstatQacctPoller.QacctSupport.parseMultiTaskQacctResults(idsToLookFor)(jobNumber -> lines)
    
    val expected = Map(tid -> CommandResult(0))
    
    assert(actual.toMap === expected)
  }
  
  test("poll - happy path") {
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val qstatInvocationFn: CommandInvoker.InvocationFn[Unit] = { _ => 
      Success(RunResults.Successful("MOCK_QSTAT", qstatLines, Nil))
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
        
    assert(QstatSupport.parseQstatOutput(qstatLines).toList === expected)
    
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
    
    assert(actual.forall { 
      case Success((drmTaskId, drmStatus)) => drmStatus.isUndetermined
      case _ => false
    })
    
    assert(actual.size === 2)
  }
  
  test("getByTaskId - happy path") {
    val expected = Map(
      DrmTaskId("19115592", 2) -> Success(DrmStatus.Running),
      DrmTaskId("19115592", 1) -> Success(DrmStatus.Running))
        
    assert(QstatSupport.getByTaskId(qstatLines) === expected)
    
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
    
    assert(parseMultiTaskQacctResults(Set(tid0, tid1, tid2))(jobId -> output).toMap === expected)
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
    
    assert(parseMultiTaskQacctResults(Set(tid0, tid1))(jobId -> output).toMap === expected)
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
    
    assert(parseMultiTaskQacctResults(Set(tid0, tid1))(jobId -> missingField).toMap === Map.empty) 
  
    assert(parseMultiTaskQacctResults(Set(tid0, tid1))(jobId -> brokenField).toMap === Map.empty)
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
    
    assert(parseMultiTaskQacctResults(Set(tid0, tid1, tid2))(jobId -> lines).toMap === expected) 
  }

  // scalastyle:off line.size.limit
  private val problematicQacctOutput: String = {
    """qname        broad               
hostname     uger-c024.broadinstitute.org
group        broad               
owner        diguser             
project      broad               
department   defaultdepartment   
jobname      loamstream1841233964035718821.sh
jobnumber    19290502            
taskid       1                   
account      sge                 
priority     0                   
cwd          /humgen/diabetes2/users/dig/loamstream/ci/jenkins/home/workspace/ls-integration-tests-branch
submit_host  dig-ae-dev-01.broadinstitute.org
submit_cmd   qsub -cwd -shell y -b n -si 78 -t 1-1 -binding linear:1 -pe smp 1 -q broad -l h_rt=2:0:0,h_vmem=1G -o .loamstream/uger/LoamStream-948ff2b7-bef7-4496-8b7e-0441d60a83e3.$JOB_ID.$TASK_ID.stdout -e .loamstream/uger/LoamStream-948ff2b7-bef7-4496-8b7e-0441d60a83e3.$JOB_ID.$TASK_ID.stderr /humgen/diabetes2/users/dig/loamstream/ci/jenkins/home/workspace/ls-integration-tests-branch/.loamstream/uger/loamstream1841233964035718821.sh
qsub_time    08/06/2020 20:38:52.994
start_time   08/06/2020 20:39:06.612
end_time     08/06/2020 20:39:14.776
granted_pe   smp                 
slots        1                   
failed       0    
deleted_by   NONE
exit_status  0                   
ru_wallclock 8.164        
ru_utime     1.312        
ru_stime     0.607        
ru_maxrss    32524               
ru_ixrss     0                   
ru_ismrss    0                   
ru_idrss     0                   
ru_isrss     0                   
ru_minflt    101211              
ru_majflt    35                  
ru_nswap     0                   
ru_inblock   42016               
ru_oublock   72                  
ru_msgsnd    0                   
ru_msgrcv    0                   
ru_nsignals  0                   
ru_nvcsw     8221                
ru_nivcsw    10                  
wallclock    9.182        
cpu          1.919        
mem          0.082             
io           0.009             
iow          3.070             
ioops        4014                
maxvmem      320.895M
maxrss       0.000
maxpss       0.000
arid         undefined
jc_name      NONE"""
  }
  // scalastyle:on line.size.limit
}
