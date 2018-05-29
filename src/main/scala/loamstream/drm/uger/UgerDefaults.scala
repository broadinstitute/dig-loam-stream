package loamstream.drm.uger

import loamstream.drm.Queue
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory

/**
 * @author clint
 * Oct 11, 2017
 */
object UgerDefaults {
  val maxConcurrentJobs: Int = 2000 //scalastyle:ignore magic.number
  
  val cores: Cpus = Cpus(1)

  val memoryPerCore: Memory = Memory.inGb(1)
    
  val maxRunTime: CpuTime = CpuTime.inHours(2)
  
  val queue: Queue = Queue("broad")
}
