package loamstream.drm.uger

import loamstream.drm.Queue
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import java.nio.file.Paths
import java.nio.file.Path
import loamstream.drm.DrmDefaults

/**
 * @author clint
 * Oct 11, 2017
 */
object UgerDefaults extends DrmDefaults {
  val queue: Queue = Queue("broad")
  
  val extraPathDir: Path = Paths.get("/humgen/diabetes/users/dig/miniconda2/bin")

  val condaEnvName: String = "loamstream_v1.0"
  
  val staticJobSubmissionParams: String = "-cwd -shell y -b n"
  
  val maxQacctCacheSize: Int = 100
}
