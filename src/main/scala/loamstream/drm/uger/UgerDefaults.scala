package loamstream.drm.uger

import loamstream.drm.Queue
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import java.nio.file.Paths
import java.nio.file.Path

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
  
  val extraPathDir: Path = Paths.get("/humgen/diabetes/users/dig/miniconda2/bin")

  val condaEnvName: String = "loamstream_v1.0"
  
  val staticJobSubmissionParams: String = "-cwd -shell y -b n"
}
