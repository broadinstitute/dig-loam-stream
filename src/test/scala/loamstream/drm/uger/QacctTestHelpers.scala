package loamstream.drm.uger

import java.time.LocalDateTime
import java.time.ZonedDateTime

import scala.util.Success
import scala.util.Try

import loamstream.drm.Queue
import loamstream.model.execute.Resources.UgerResources
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.util.RunResults

/**
 * @author clint
 * Mar 15, 2017
 */
object QacctTestHelpers {
  def successfulRun(
      stdout: Seq[String], 
      stderr: Seq[String] = Nil, 
      fakeBinaryName: String = "MOCK"): Try[RunResults] = {
    
    Success(RunResults(fakeBinaryName, exitCode = 0, stdout = stdout, stderr = stderr))
  }
  
  def failedRun(
      exitCode: Int, 
      stdout: Seq[String], 
      stderr: Seq[String] = Nil, 
      fakeBinaryName: String = "MOCK"): Try[RunResults] = {

    Success(RunResults(fakeBinaryName, exitCode = exitCode, stdout = stdout, stderr = stderr))
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
      expectedEndTime: LocalDateTime): Seq[String] = s"""
qname        ${queue.map(_.name).getOrElse("")}
hostname     ${node.getOrElse("")}
group        broad
owner        cgilbert
project      broad
department   defaultdepartment
jobname      LoamStream-aad659d0-f261-4e02-a78b-90755cb2a9d7
jobnumber    6436107
taskid       1
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
exit_status  0
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
    """.trim.split("[\\r\\n]+")
  //scalastyle:on method.length
    
  def multiJobQacctOutput(jobName: String, jobNumber: Int): String = s"""
==============================================================
qname        broad               
hostname     uger-c100.broadinstitute.org
group        broad               
owner        cgilbert            
project      broad               
department   defaultdepartment   
jobname      ${jobName}
jobnumber    ${jobNumber}            
taskid       3                   
account      sge                 
priority     0                   
cwd          /humgen/diabetes2/users/cgilbert/tmp
submit_host  dig-ae-dev-01.broadinstitute.org
submit_cmd   drmaa -p 0 -b y -shell n -w e -q broad -P broad -binding linear:1 -l h_rt=2:0:0 -r n -wd -wd -shell y -b n -binding linear:1 -pe smp 1 -q broad -l h_rt=2:0:0,h_vmem=1G -N ${jobName} -o :.loamstream/uger/${jobName}.$$drmaa_incr_ph$$.stdout -e :.loamstream/uger/${jobName}.$$drmaa_incr_ph$$.stderr /humgen/diabetes2/users/cgilbert/tmp/.loamstream/uger/loamstream7326045379935757093.sh -t 1-3:1
qsub_time    04/09/2020 17:31:37.136
start_time   04/09/2020 17:31:44.770
end_time     04/09/2020 17:31:47.926
granted_pe   smp                 
slots        1                   
failed       0    
deleted_by   NONE
exit_status  0                   
ru_wallclock 3.156        
ru_utime     1.159        
ru_stime     0.528        
ru_maxrss    32456               
ru_ixrss     0                   
ru_ismrss    0                   
ru_idrss     0                   
ru_isrss     0                   
ru_minflt    100939              
ru_majflt    35                  
ru_nswap     0                   
ru_inblock   42016               
ru_oublock   40                  
ru_msgsnd    0                   
ru_msgrcv    0                   
ru_nsignals  0                   
ru_nvcsw     8041                
ru_nivcsw    15                  
wallclock    3.373        
cpu          1.688        
mem          0.015             
io           0.016             
iow          0.000             
ioops        6228                
maxvmem      412.043M
maxrss       0.000
maxpss       0.000
arid         undefined
jc_name      NONE
==============================================================
qname        broad               
hostname     ugertmp-c017.broadinstitute.org
group        broad               
owner        cgilbert            
project      broad               
department   defaultdepartment   
jobname      ${jobName}
jobnumber    ${jobNumber}            
taskid       2                   
account      sge                 
priority     0                   
cwd          /humgen/diabetes2/users/cgilbert/tmp
submit_host  dig-ae-dev-01.broadinstitute.org
submit_cmd   drmaa -p 0 -b y -shell n -w e -q broad -P broad -binding linear:1 -l h_rt=2:0:0 -r n -wd -wd -shell y -b n -binding linear:1 -pe smp 1 -q broad -l h_rt=2:0:0,h_vmem=1G -N ${jobName} -o :.loamstream/uger/${jobName}.$$drmaa_incr_ph$$.stdout -e :.loamstream/uger/${jobName}.$$drmaa_incr_ph$$.stderr /humgen/diabetes2/users/cgilbert/tmp/.loamstream/uger/loamstream7326045379935757093.sh -t 1-3:1
qsub_time    04/09/2020 17:31:37.136
start_time   04/09/2020 17:31:44.775
end_time     04/09/2020 17:31:49.073
granted_pe   smp                 
slots        1                   
failed       0    
deleted_by   NONE
exit_status  0                   
ru_wallclock 4.298        
ru_utime     1.074        
ru_stime     0.521        
ru_maxrss    32460               
ru_ixrss     0                   
ru_ismrss    0                   
ru_idrss     0                   
ru_isrss     0                   
ru_minflt    100889              
ru_majflt    36                  
ru_nswap     0                   
ru_inblock   42136               
ru_oublock   40                  
ru_msgsnd    0                   
ru_msgrcv    0                   
ru_nsignals  0                   
ru_nvcsw     8093                
ru_nivcsw    29                  
wallclock    4.572        
cpu          1.595        
mem          0.005             
io           0.015             
iow          0.000             
ioops        5918                
maxvmem      369.449M
maxrss       0.000
maxpss       0.000
arid         undefined
jc_name      NONE
==============================================================
qname        broad               
hostname     uger-c046.broadinstitute.org
group        broad               
owner        cgilbert            
project      broad               
department   defaultdepartment   
jobname      ${jobName}
jobnumber    ${jobNumber}            
taskid       1                   
account      sge                 
priority     0                   
cwd          /humgen/diabetes2/users/cgilbert/tmp
submit_host  dig-ae-dev-01.broadinstitute.org
submit_cmd   drmaa -p 0 -b y -shell n -w e -q broad -P broad -binding linear:1 -l h_rt=2:0:0 -r n -wd -wd -shell y -b n -binding linear:1 -pe smp 1 -q broad -l h_rt=2:0:0,h_vmem=1G -N ${jobName} -o :.loamstream/uger/${jobName}.$$drmaa_incr_ph$$.stdout -e :.loamstream/uger/${jobName}.$$drmaa_incr_ph$$.stderr /humgen/diabetes2/users/cgilbert/tmp/.loamstream/uger/loamstream7326045379935757093.sh -t 1-3:1
qsub_time    04/09/2020 17:31:37.136
start_time   04/09/2020 17:31:44.824
end_time     04/09/2020 17:31:49.090
granted_pe   smp                 
slots        1                   
failed       0    
deleted_by   NONE
exit_status  0                   
ru_wallclock 4.266        
ru_utime     1.265        
ru_stime     0.662        
ru_maxrss    32452               
ru_ixrss     0                   
ru_ismrss    0                   
ru_idrss     0                   
ru_isrss     0                   
ru_minflt    100905              
ru_majflt    35                  
ru_nswap     0                   
ru_inblock   42016               
ru_oublock   40                  
ru_msgsnd    0                   
ru_msgrcv    0                   
ru_nsignals  0                   
ru_nvcsw     8035                
ru_nivcsw    420                 
wallclock    4.619        
cpu          1.926        
mem          0.006             
io           0.000             
iow          0.000             
ioops        369                 
maxvmem      579.188M
maxrss       0.000
maxpss       0.000
arid         undefined
jc_name      NONE
""".trim
}
