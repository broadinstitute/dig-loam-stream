package loamstream.drm.slurm

import loamstream.model.quantities.Cpus
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Memory
import loamstream.drm.DrmDefaults

/**
 * @author clint
 * May 10, 2018
 */
object SlurmDefaults extends DrmDefaults {
  val maxSacctRetries: Int = maxRetries
}
