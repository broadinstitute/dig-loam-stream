package loamstream.drm.lsf

import loamstream.drm.Queue
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory


/**
 * @author clint
 * May 10, 2018
 */
object LsfDefaults {
  //TODO: determine what this should be
  val maxNumJobsPerTaskArray: Int = 2000 //scalastyle:ignore magic.number
  
  val cores: Cpus = Cpus(1)

  val memoryPerCore: Memory = Memory.inGb(1)
    
  val maxRunTime: CpuTime = CpuTime.inHours(2)
  
  val maxBacctRetries: Int = 9
  
  val maxJobSubmissionRetries: Int = 2
}
