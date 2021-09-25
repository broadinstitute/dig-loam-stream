package loamstream.drm.lsf

import loamstream.drm.Queue
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.drm.DrmDefaults


/**
 * @author clint
 * May 10, 2018
 */
object LsfDefaults extends DrmDefaults {
  val maxBacctRetries: Int = maxRetries
}
