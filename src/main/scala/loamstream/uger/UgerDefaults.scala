package loamstream.uger

import loamstream.model.quantities.Memory
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import scala.concurrent.duration.Duration

/**
 * @author clint
 * Oct 11, 2017
 */
object UgerDefaults {
  val cores: Cpus = Cpus(1)

  val memoryPerCore: Memory = Memory.inGb(1)
    
  val maxRunTime: CpuTime = CpuTime.inHours(2)
  
  val queue: Queue = Queue.Broad
  
  val maxWaitTimeForOutputs: Duration = {
    import scala.concurrent.duration._
    
    30.seconds
  }
}
