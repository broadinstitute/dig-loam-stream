package loamstream.uger

import loamstream.drm.Queue

/**
 * @author clint
 * Mar 15, 2017
 */
object QacctTestHelpers {
  //scalastyle:off method.length
  def actualQacctOutput(queue: Option[Queue], node: Option[String]): Seq[String] = s"""
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
start_time   03/06/2017 17:49:50.505
end_time     03/06/2017 17:49:57.464
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
}
