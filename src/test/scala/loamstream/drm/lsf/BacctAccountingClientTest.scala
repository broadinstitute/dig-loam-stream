package loamstream.drm.lsf

import org.scalatest.FunSuite
import loamstream.model.execute.Resources.LsfResources
import loamstream.model.quantities.Memory
import scala.util.Success
import java.time.Instant
import loamstream.drm.Queue
import loamstream.model.quantities.CpuTime
import java.time.ZoneId
import java.time.temporal.ChronoField
import java.time.ZonedDateTime
import loamstream.util.RetryingCommandInvoker
import loamstream.util.RunResults
import scala.util.Try

/**
 * @author clint
 * Apr 18, 2019
 */
final class BacctAccountingClientTest extends FunSuite {
  private def runResultsAttempt(
      binaryName: String = "MOCK", 
      exitCode: Int = 0, 
      stdout: Seq[String] = Nil,
      stderr: Seq[String] = Nil): Try[RunResults] = Success(RunResults(binaryName, exitCode, stdout, stderr))
  
  test("Parse actual bacct outpout - bad input") {
    def doTest(bacctOutput: Seq[String]): Unit = {
      val mockInvoker = new RetryingCommandInvoker[String](0, "MOCK", _ => runResultsAttempt(stdout = bacctOutput))
    
      assert(new BacctAccountingClient(mockInvoker).getResourceUsage("foo").isFailure === true)
    }
    
    doTest(Nil)
    doTest(Seq("x", "y", "z"))
  }
  
  test("Parse actual bacct outpout - happy path") {
    val splitOutput = actualOutput.split("\\n")
    
    val mockInvoker = new RetryingCommandInvoker[String](0, "MOCK", _ => runResultsAttempt(stdout = splitOutput))
    
    val actual = (new BacctAccountingClient(mockInvoker)).getResourceUsage("someJobId")
    
    val now = ZonedDateTime.now
    
    val systemTimeZoneOffSet = now.getOffset.getId
    
    val currentYear: Int = now.get(ChronoField.YEAR)
    
    val expected = LsfResources(
            Memory.inMb(123), 
            CpuTime.inSeconds(0.02), 
            Option("ebi6-054"),
            Option(Queue("research-rh7")),
            ZonedDateTime.parse(s"${currentYear}-04-18T22:32:01.00${systemTimeZoneOffSet}").toInstant,
            ZonedDateTime.parse(s"${currentYear}-04-18T23:34:12.00${systemTimeZoneOffSet}").toInstant)
    
    assert(actual.get === expected)
  }
  
  test("Parse actual bacct outpout - problematic output") {
    val splitOutput = problematicOutput.split("\\n")
    
    val mockInvoker = new RetryingCommandInvoker[String](0, "MOCK", _ => runResultsAttempt(stdout = splitOutput))
    
    val actual = (new BacctAccountingClient(mockInvoker)).getResourceUsage("someJobId")
    
    val now = ZonedDateTime.now
    
    val systemTimeZoneOffSet = now.getOffset.getId
    
    val currentYear: Int = now.get(ChronoField.YEAR)
    
    val expected = LsfResources(
            Memory.inMb(28), 
            CpuTime.inSeconds(0.58), 
            Option("ebi5-153"),
            Option(Queue("research-rh7")),
            ZonedDateTime.parse(s"${currentYear}-05-01T22:42:24.00${systemTimeZoneOffSet}").toInstant,
            ZonedDateTime.parse(s"${currentYear}-05-01T22:42:46.00${systemTimeZoneOffSet}").toInstant)
    
    assert(actual.get === expected)
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
  
  test("parseTimestamp") {
    import BacctAccountingClient.parseTimestamp
    import BacctAccountingClient.Regexes
    
    val now = ZonedDateTime.now
    
    val systemTimeZoneOffSet = now.getOffset.getId
    
    val currentYear: Int = now.get(ChronoField.YEAR)

    val april18 = ZonedDateTime.parse(s"${currentYear}-04-18T22:32:01.00${systemTimeZoneOffSet}").toInstant
    val april1 = ZonedDateTime.parse(s"${currentYear}-04-01T22:32:01.00${systemTimeZoneOffSet}").toInstant
    
    def doTest(dateString: String, expected: Instant): Unit = {
      def parse(s: String) = parseTimestamp(Seq(s))(Regexes.startTime, "start")
      
      val attempt0 = parse(s"${dateString}: [1] dispatched to asdasdasdads")
      val attempt1 = parse(s"${dateString}: Dispatched to asdasdasdads")
      
      assert(attempt0.get === expected)
      assert(attempt1.get === expected)
    }
    
    doTest("Thu Apr 18 22:32:01", april18)
    doTest("Mon Apr  1 22:32:01", april1)
  }
  
  private val actualOutput = """
    Accounting information about jobs that are: 
  - submitted by all users.
  - accounted on all projects.
  - completed normally or exited
  - executed on all hosts.
  - submitted to all queues.
  - accounted on all service classes.
------------------------------------------------------------------------------

Job <2811237>, User <cgilbert>, Project <default>, Status <EXIT>, Queue <research-rh7>, Command <cp ./A.txt ./B.txt>, Share group charged </cgilbert>
Thu Apr 18 22:32:01: Submitted from host <ebi-cli-001>, CWD <$HOME>;
Thu Apr 18 22:32:01: Dispatched to <ebi6-054>, Effective RES_REQ <select[type == local] order[r15s:pg] rusage[numcpus=1.00:duration=8h:decay=0] span[hosts=1] >;
Thu Apr 18 23:34:12: Completed <exit>.

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
 Total Run time consumed:         0      Average Run time consumed:       0
    """
  
  private val problematicOutput = """
    Accounting information about jobs that are:
   - submitted by all users.
   - accounted on all projects.
   - completed normally or exited
   - executed on all hosts.
   - submitted to all queues.
   - accounted on all service classes.
   ------------------------------------------------------------------------------
   
   Job <224706[1]>, Job Name <LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236[1]>, User <cgilbert>, Project <default>, Status <DONE>, Queue <research-rh7>, Command <#!/bin/bash;  i=$LSB_JOBINDEX;jobId=$LSB_JOBID;       if [ $i -eq 1 ];then;java -Xms1g -Xmx1g Hello 1000000 20 && touch ./X.txt; LOAMSTREAM_JOB_EXIT_CODE=$?; stdoutDestPath="/homes/cgilbert/ls/.loamstream/job-outputs/_anon_tool_name-0.stdout";stderrDestPath="/homes/cgilbert/ls/.loamstream/job-outputs/_anon_tool_name-0.stderr"; mkdir -p /homes/cgilbert/ls/.loamstream/job-outputs;mv /homes/cgilbert/ls/.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.1.stdout $stdoutDestPath || echo "Couldn't move DRM std out log /homes/cgilbert/ls/.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.1.stdout; it's likely the job wasn't submitted successfully" > $stdoutDestPath;mv /homes/cgilbert/ls/.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.1.stderr $stderrDestPath || echo "Couldn't move DRM std err log /homes/cgilbert/ls/.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.1.stderr; it's likely the job wasn't submitted successfully" > $stderrDestPath; exit $LOAMSTREAM_JOB_EXIT_CODE;  fi>, Share group charged </cgilbert> 
   Wed May  1 22:42:23: Submitted from host <ebi-cli-003>, CWD <$HOME/ls> Output File (overwrite) <.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.%I.stdout>, Error File (overwrite) <.loamstream/lsf/LoamStream-1b97b895-2f8e-46ea-8a35-6f9908018236.%I.stderr>;
   Wed May  1 22:42:24: [1] dispatched to <ebi5-153>, Effective RES_REQ <select[type == local] order[r15s:pg] rusage[mem=1000.00:duration=8h:decay=0,numcpus=1.00:duration=8h:decay=0] span[hosts=1] >;
   Wed May  1 22:42:46: Completed <done>.
   
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
   Maximum Run time of a job:      22      Minimum Run time of a job:      22
   """
}
