package loamstream.uger

import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus

/**
 * @author clint
 * Oct 11, 2017
 */
object UgerDefaults {
  val cores: Cpus = Cpus(1)

  val memoryPerCore: Memory = Memory.inGb(1)
    
  val maxRunTime: CpuTime = CpuTime.inHours(2)
  
  val queue: Queue = Queue.Broad
}
