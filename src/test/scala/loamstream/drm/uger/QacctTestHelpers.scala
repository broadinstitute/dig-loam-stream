package loamstream.drm.uger

import java.time.LocalDateTime

import scala.util.Success
import scala.util.Try
import loamstream.drm.Queue
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.RunResults
import loamstream.util.ExitCodes

import scala.collection.immutable.ArraySeq

/**
 * @author clint
 * Mar 15, 2017
 */
object QacctTestHelpers {
  def successfulRun(
      stdout: Seq[String], 
      stderr: Seq[String] = Nil, 
      fakeBinaryName: String = "MOCK"): Try[RunResults] = {
    
    Success(
        RunResults(fakeBinaryName, exitCode = 0, stdout = stdout, stderr = stderr, isSuccess = ExitCodes.isSuccess))
  }
  
  def failedRun(
      exitCode: Int, 
      stdout: Seq[String], 
      stderr: Seq[String] = Nil, 
      fakeBinaryName: String = "MOCK"): Try[RunResults] = {

    Success(RunResults(
        fakeBinaryName, exitCode = exitCode, stdout = stdout, stderr = stderr, isSuccess = ExitCodes.isSuccess))
  }
  
  def expectedResources(
      expectedRawData: String, 
      expectedNode: String, 
      expectedQueue: Queue,
      expectedStartTime: LocalDateTime,
      expectedEndTime: LocalDateTime): UgerResources = {
    
    expectedResources(expectedRawData, Option(expectedNode), Option(expectedQueue), expectedStartTime, expectedEndTime)
  }

  def expectedResources(
      expectedRawData: String, 
      expectedNode: Option[String], 
      expectedQueue: Option[Queue],
      expectedStartTime: LocalDateTime,
      expectedEndTime: LocalDateTime): UgerResources = {

    UgerResources(
      memory = Memory.inKb(60092),
      cpuTime = CpuTime.inSeconds(2.487),
      node = expectedNode,
      queue = expectedQueue,
      startTime = expectedStartTime,
      endTime = expectedEndTime,
      raw = Option(expectedRawData))
  }
  
  def toUgerFormat(i: LocalDateTime): String = QacctAccountingClient.dateFormatter.format(i)
  
  //scalastyle:off method.length
  def actualQacctOutput(
      queue: Option[Queue], 
      node: Option[String],
      expectedStartTime: LocalDateTime,
      expectedEndTime: LocalDateTime,
      exitCode: Int = 0,
      jobNumber: String = "6436107",
      taskIndex: Int = 1): Seq[String] = ArraySeq.unsafeWrapArray(s"""
qname        ${queue.map(_.name).getOrElse("")}
hostname     ${node.getOrElse("")}
group        broad
owner        cgilbert
project      broad
department   defaultdepartment
jobname      LoamStream-aad659d0-f261-4e02-a78b-90755cb2a9d7
jobnumber    ${jobNumber}
taskid       ${taskIndex}
account      sge
priority     0
cwd          /humgen/diabetes/users/cgilbert/run-dir/extra-uger-info
submit_host  hw-uger-1003.broadinstitute.org
submit_cmd   NONE
qsub_time    03/06/2017 17:49:46.288
start_time   ${toUgerFormat(expectedStartTime)}
end_time     ${toUgerFormat(expectedEndTime)}
granted_pe   NONE
slots        1
failed       0
deleted_by   NONE
exit_status  ${exitCode}
ru_wallclock 6.959
ru_utime     2.248
ru_stime     0.239
ru_maxrss    60092
ru_ixrss     0
ru_ismrss    0
ru_idrss     0
ru_isrss     0
ru_minflt    60230
ru_majflt    38
ru_nswap     0
ru_inblock   46952
ru_oublock   784
ru_msgsnd    0
ru_msgrcv    0
ru_nsignals  0
ru_nvcsw     2113
ru_nivcsw    1014
wallclock    7.146
cpu          2.487
mem          0.093
io           0.010
iow          0.280
ioops        2120
maxvmem      322.164M
maxrss       0.000
maxpss       0.000
arid         undefined
jc_name      NONE
    """.trim.split("[\\r\\n]+"))
  //scalastyle:on method.length
}
