package loamstream.drm.lsf

import java.time.LocalDateTime
import java.time.temporal.ChronoField

import scala.util.Success
import scala.util.Try
import org.scalatest.FunSuite
import loamstream.drm.DrmTaskId
import loamstream.drm.Queue
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.jobs.TerminationReason
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.CommandInvoker
import loamstream.util.RunResults
import rx.lang.scala.schedulers.ComputationScheduler

import scala.collection.immutable.ArraySeq

/**
 * @author clint
 * Apr 18, 2019
 */
final class BacctAccountingClientTest extends FunSuite {
  import loamstream.TestHelpers.waitFor
  
  private def runResultsAttempt(
      binaryName: String = "MOCK", 
      exitCode: Int = 0, 
      stdout: Seq[String] = Nil,
      stderr: Seq[String] = Nil): Try[RunResults] = Success(RunResults(binaryName, exitCode, stdout, stderr))
  
  private def asTrimmedLines(s: String): Seq[String] = ArraySeq.unsafeWrapArray(s.split("\\n").map(_.trim))
  
  private val now = LocalDateTime.now
  private val currentYear: Int = now.get(ChronoField.YEAR)
  
  import scala.concurrent.ExecutionContext.Implicits.global
  
  test("Parse actual bacct outpout - bad input") {
    def doTest(bacctOutput: Seq[String]): Unit = {
      val mockInvoker = CommandInvoker.Async.Retrying[DrmTaskId](
          0, 
          "MOCK", 
          _ => runResultsAttempt(stdout = bacctOutput), 
          scheduler = ComputationScheduler())

      val taskId = DrmTaskId("foo", 42)
      
      waitFor(new BacctAccountingClient(mockInvoker).getResourceUsage(taskId).failed)
    }
    
    doTest(Nil)
    doTest(Seq("x", "y", "z"))
  }
  
  test("Parse actual bacct outpout - happy path") {
    val splitOutput = ArraySeq.unsafeWrapArray(actualOutput.split("\\n"))
    
    val mockInvoker = CommandInvoker.Async.Retrying[DrmTaskId](
        0, 
        "MOCK", 
        _ => runResultsAttempt(stdout = splitOutput),
        scheduler = ComputationScheduler())
    
    val taskId = DrmTaskId("someJobId", 42)
    
    val actual = waitFor((new BacctAccountingClient(mockInvoker)).getResourceUsage(taskId))
    
    val expected = LsfResources(
            Memory.inMb(123), 
            CpuTime.inSeconds(0.02), 
            Option("ebi6-054"),
            Option(Queue("research-rh7")),
            LocalDateTime.parse(s"${currentYear}-04-18T22:32:01.00"),
            LocalDateTime.parse(s"${currentYear}-04-18T23:34:12.00"),
            raw = Option(actualOutput))
    
    assert(actual.memory === expected.memory)
    assert(actual.cpuTime === expected.cpuTime)
    assert(actual.node === expected.node)
    assert(actual.queue === expected.queue)
    assert(actual.startTime === expected.startTime)
    assert(actual.endTime === expected.endTime)
    
    assert(asTrimmedLines(actual.raw.get) === asTrimmedLines(expected.raw.get))
  }
  
  test("getTerminationReason - happy path") {
    def doTest(lsfReason: String, lsfDesc: String, expected: Option[TerminationReason]): Unit = {
      val rawOutput = actualOutputWithTerminationReason(lsfReason, lsfDesc)
      
      val splitOutput = ArraySeq.unsafeWrapArray(rawOutput.split("\\n"))
      
      val mockInvoker = CommandInvoker.Async.Retrying[DrmTaskId](
          0, 
          "MOCK", 
          _ => runResultsAttempt(stdout = splitOutput),
          scheduler = ComputationScheduler())
      
      val taskId = DrmTaskId("someJobId", 42)
      
      val actual = waitFor((new BacctAccountingClient(mockInvoker)).getTerminationReason(taskId))
      
      assert(actual === expected)
    }
    
    doTest("", "blah blah", None)
    doTest("asdfasdf", "blah blah", None)
    
    doTest("TERM_RUNLIMIT", "blah blah", Some(TerminationReason.RunTime))
    
    doTest("TERM_CPULIMIT", "blah blah", Some(TerminationReason.CpuTime))
    
    doTest("TERM_OWNER", "blah blah", Some(TerminationReason.UserRequested))
    doTest("TERM_FORCE_OWNER", "blah blah", Some(TerminationReason.UserRequested))
    
    doTest("TERM_MEMLIMIT", "blah blah", Some(TerminationReason.Memory))
    doTest("TERM_SWAP", "blah blah", Some(TerminationReason.Memory))
    
    doTest("TERM_UNKNOWN", "blah blah", Some(TerminationReason.Unknown))
    doTest("TERM_FOO", "blah blah", Some(TerminationReason.Unknown))
  }
  
  test("Parse actual bacct outpout - problematic output") {
    val splitOutput = ArraySeq.unsafeWrapArray(problematicOutput.split("\\n"))
    
    val mockInvoker = CommandInvoker.Async.Retrying[DrmTaskId](
        0, 
        "MOCK", 
        _ => runResultsAttempt(stdout = splitOutput),
        scheduler = ComputationScheduler())
    
    val taskId = DrmTaskId("someJobId", 42)
    
    val actual = waitFor((new BacctAccountingClient(mockInvoker)).getResourceUsage(taskId))
    
    val expected = LsfResources(
            Memory.inMb(28), 
            CpuTime.inSeconds(0.58), 
            Option("ebi5-153"),
            Option(Queue("research-rh7")),
            LocalDateTime.parse(s"${currentYear}-05-01T22:42:24.00"),
            LocalDateTime.parse(s"${currentYear}-05-01T22:42:46.00"),
            raw = Option(problematicOutput))
    
    assert(actual.memory === expected.memory)
    assert(actual.cpuTime === expected.cpuTime)
    assert(actual.node === expected.node)
    assert(actual.queue === expected.queue)
    assert(actual.startTime === expected.startTime)
    assert(actual.endTime === expected.endTime)
    
    assert(asTrimmedLines(actual.raw.get) === asTrimmedLines(expected.raw.get))
  }
  
  test("parseMemory") {
    import BacctAccountingClient.parseMemory
    
    val line = "doesn't matter"
    
    assert(parseMemory(line)("123M").get === Memory.inMb(123))
    assert(parseMemory(line)("123m").get === Memory.inMb(123))
    assert(parseMemory(line)("1.23M").get === Memory.inMb(1.23))
    assert(parseMemory(line)("1.23m").get === Memory.inMb(1.23))
    
    assert(parseMemory(line)("32G").get === Memory.inGb(32))
    assert(parseMemory(line)("32g").get === Memory.inGb(32))
    assert(parseMemory(line)("3.2G").get === Memory.inGb(3.2))
    assert(parseMemory(line)("3.2g").get === Memory.inGb(3.2))
  }
  
  test("parse{Start,End}Time") {
    import BacctAccountingClient.parseStartTime
    import BacctAccountingClient.parseEndTime
    
    val dayOfWeek18 = dayOfWeek("04-18")
    val dayOfWeek1 = dayOfWeek("04-01")
    
    val april18 = LocalDateTime.parse(s"${currentYear}-04-18T22:32:01.00")
    val april1 = LocalDateTime.parse(s"${currentYear}-04-01T22:32:01.00")
    
    def doTest(
        date: String, 
        expected: LocalDateTime, 
        dateLinePart: String, 
        parse: Seq[String] => Try[LocalDateTime]): Unit = {
      
      assert(parse(Seq(s"${date}: ${dateLinePart} asdasdasdads")) === Success(expected))
    }
    
    doTest(s"${dayOfWeek18} Apr 18 22:32:01", april18, "[1] dispatched to", parseStartTime)
    doTest(s"${dayOfWeek18} Apr 18 22:32:01", april18, "Dispatched to", parseStartTime)
    doTest(s"${dayOfWeek1} Apr  1 22:32:01", april1, "[1] dispatched to", parseStartTime)
    doTest(s"${dayOfWeek1} Apr  1 22:32:01", april1, "Dispatched to", parseStartTime)
    
    doTest(s"${dayOfWeek18} Apr 18 22:32:01", april18, "Completed", parseEndTime)
    doTest(s"${dayOfWeek1} Apr  1 22:32:01", april1, "Completed", parseEndTime)

    def doTestShouldFail(line: String, parse: Seq[String] => Try[LocalDateTime]): Unit = {
      assert(parse(Seq(line)).isFailure)
    }
    
    doTestShouldFail("Thu Apr 18 22:32:01: [1] dispatched to", parseEndTime)
    doTestShouldFail("Thu Apr 18 22:32:01: Dispatched to", parseEndTime)
    doTestShouldFail("Mon Apr  1 22:32:01: [1] dispatched to", parseEndTime)
    doTestShouldFail("Mon Apr  1 22:32:01: Dispatched to", parseEndTime)
    
    doTestShouldFail("Thu Apr 18 22:32:01: Completed", parseStartTime)
    doTestShouldFail("Mon Apr  1 22:32:01: Completed", parseStartTime)
    
    doTestShouldFail("asdasdasdads", parseStartTime)
    doTestShouldFail("asdasdasdads", parseEndTime)
  }
  
  private def dayOfWeek(monthAndDay: String): String = {
    import java.time.format.TextStyle
    import java.util.Locale
    
    val ldt = LocalDateTime.parse(s"${currentYear}-${monthAndDay}T00:00:00.00")
    
    ldt.getDayOfWeek.getDisplayName(TextStyle.SHORT, Locale.getDefault)
  }
  
  // scalastyle:off line.size.limit
  private val actualOutput = s"""
Accounting information about jobs that are: 
  - submitted by all users.
  - accounted on all projects.
  - completed normally or exited
  - executed on all hosts.
  - submitted to all queues.
  - accounted on all service classes.
------------------------------------------------------------------------------

Job <2811237>, User <cgilbert>, Project <default>, Status <EXIT>, Queue <research-
                     rh7>, Command <cp ./A.txt ./B.txt>, Share group charged </cgi
                     lbert>
${dayOfWeek("04-18")} Apr 18 22:32:01: Submitted from host <ebi-cli-001>, CWD <$$HOME>;
${dayOfWeek("04-18")} Apr 18 22:32:01: Dispatched to <ebi6-054>, Effective RES_REQ <select[type == l
                     ocal] order[r15s:pg] rusage[numcpus=1.00:duration=8h:decay=0]
                      span[hosts=1] >;
${dayOfWeek("04-18")} Apr 18 23:34:12: Completed <exit>.

Accounting information about this job:
     Share group charged </cgilbert>
     CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
      0.02        0              0     exit         0.0000     123M      3.2G
------------------------------------------------------------------------------

SUMMARY:      ( time unit: second ) 
 Total number of done jobs:       0      Total number of exited jobs:     1
 Total CPU time consumed:       0.0      Average CPU time consumed:     0.0
 Maximum CPU time of a job:     0.0      Minimum CPU time of a job:     0.0
 Total wait time in queues:     0.0
 Average wait time in queue:    0.0
 Maximum wait time in queue:    0.0      Minimum wait time in queue:    0.0
 Average turnaround time:         0 (seconds/job)
 Maximum turnaround time:         0      Minimum turnaround time:         0
 Average hog factor of a job:  0.00 ( cpu time / turnaround time )
 Maximum hog factor of a job:  0.00      Minimum hog factor of a job:  0.00
 Average expansion factor of a job:  0.00 ( turnaround time / run time )
 Maximum expansion factor of a job:  0.00
 Minimum expansion factor of a job:  0.00
 Total Run time consumed:         0      Average Run time consumed:       0""".trim

  private def actualOutputWithTerminationReason(termReason: String, termDesc: String): String = s"""
Accounting information about jobs that are: 
  - submitted by all users.
  - accounted on all projects.
  - completed normally or exited
  - executed on all hosts.
  - submitted to all queues.
  - accounted on all service classes.
------------------------------------------------------------------------------

Job <2119469>, User <cgilbert>, Project <default>, Status <EXIT>, Queue <research-
                     rh7>, Command <java Hello 250000000 0>, Share group charged <
                     /cgilbert>
Mon May  6 22:58:01: Submitted from host <ebi-cli-001>, CWD <$$HOME>;
Mon May  6 22:58:03: Dispatched to <ebi6-142>, Effective RES_REQ <select[type == l
                     ocal] order[r15s:pg] rusage[mem=1000.00:duration=8h:decay=0,n
                     umcpus=1.00:duration=8h:decay=0] span[hosts=1] >;
Mon May  6 22:58:03: Completed <exit>; ${termReason}: ${termDesc}

Accounting information about this job:
     Share group charged </cgilbert>
     CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
      0.09        2              2     exit         0.0460     3M     60M
------------------------------------------------------------------------------

SUMMARY:      ( time unit: second ) 
 Total number of done jobs:       0      Total number of exited jobs:     1
 Total CPU time consumed:       0.1      Average CPU time consumed:     0.1
 Maximum CPU time of a job:     0.1      Minimum CPU time of a job:     0.1
 Total wait time in queues:     2.0
 Average wait time in queue:    2.0
 Maximum wait time in queue:    2.0      Minimum wait time in queue:    2.0
 Average turnaround time:         2 (seconds/job)
 Maximum turnaround time:         2      Minimum turnaround time:         2
 Average hog factor of a job:  0.05 ( cpu time / turnaround time )
 Maximum hog factor of a job:  0.05      Minimum hog factor of a job:  0.05
 Average expansion factor of a job:  2.00 ( turnaround time / run time )
 Maximum expansion factor of a job:  2.00
 Minimum expansion factor of a job:  2.00
 Total Run time consumed:         0      Average Run time consumed:       0
 Maximum Run time of a job:       0      Minimum Run time of a job:       0""".trim
  
  private val problematicOutput = s"""
Accounting information about jobs that are:
   - submitted by all users.
   - accounted on all projects.
   - completed normally or exited
   - executed on all hosts.
   - submitted to all queues.
   - accounted on all service classes.
   ------------------------------------------------------------------------------
   
   Job <224706[1]>, Job Name <LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236[1]>, User <cgilbert>, Project <default>, Status <DONE>, Queue <research-rh7>, Command <#!/bin/bash;  i=$$LSB_JOBINDEX;jobId=$$LSB_JOBID;       if [ $$i -eq 1 ];then;java -Xms1g -Xmx1g Hello 1000000 20 && touch ./X.txt; LOAMSTREAM_JOB_EXIT_CODE=$$?; stdoutDestPath="/homes/cgilbert/ls/.loamstream/job-outputs/_anon_tool_name-0.stdout";stderrDestPath="/homes/cgilbert/ls/.loamstream/job-outputs/_anon_tool_name-0.stderr"; mkdir -p /homes/cgilbert/ls/.loamstream/job-outputs;mv /homes/cgilbert/ls/.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.1.stdout $$stdoutDestPath || echo "Couldn't move DRM std out log /homes/cgilbert/ls/.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.1.stdout; it's likely the job wasn't submitted successfully" > $$stdoutDestPath;mv /homes/cgilbert/ls/.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.1.stderr $$stderrDestPath || echo "Couldn't move DRM std err log /homes/cgilbert/ls/.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.1.stderr; it's likely the job wasn't submitted successfully" > $$stderrDestPath; exit $$LOAMSTREAM_JOB_EXIT_CODE;  fi>, Share group charged </cgilbert> 
   ${dayOfWeek("05-01")} May  1 22:42:23: Submitted from host <ebi-cli-003>, CWD <$$HOME/ls> Output File (overwrite) <.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.%I.stdout>, Error File (overwrite) <.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.%I.stderr>;
   ${dayOfWeek("05-01")} May  1 22:42:24: [1] dispatched to <ebi5-153>, Effective RES_REQ <select[type == local] order[r15s:pg] rusage[mem=1000.00:duration=8h:decay=0,numcpus=1.00:duration=8h:decay=0] span[hosts=1] >;
   ${dayOfWeek("05-01")} May  1 22:42:46: Completed <done>.
   
   Accounting information about this job:
   Share group charged </cgilbert>
   CPU_T     WAIT     TURNAROUND   STATUS     HOG_FACTOR    MEM    SWAP
   0.58        1             23     done         0.0253    28M    3.2G
   ------------------------------------------------------------------------------
   
   SUMMARY:      ( time unit: second )
   Total number of done jobs:       1      Total number of exited jobs:     0
   Total CPU time consumed:       0.6      Average CPU time consumed:     0.6
   Maximum CPU time of a job:     0.6      Minimum CPU time of a job:     0.6
   Total wait time in queues:     1.0
   Average wait time in queue:    1.0
   Maximum wait time in queue:    1.0      Minimum wait time in queue:    1.0
   Average turnaround time:        23 (seconds/job)
   Maximum turnaround time:        23      Minimum turnaround time:        23
   Average hog factor of a job:  0.03 ( cpu time / turnaround time )
   Maximum hog factor of a job:  0.03      Minimum hog factor of a job:  0.03
   Average expansion factor of a job:  1.05 ( turnaround time / run time )
   Maximum expansion factor of a job:  1.05
   Minimum expansion factor of a job:  1.05
   Total Run time consumed:        22      Average Run time consumed:      22
   Maximum Run time of a job:      22      Minimum Run time of a job:      22""".trim
  // scalastyle:on line.size.limit
}
