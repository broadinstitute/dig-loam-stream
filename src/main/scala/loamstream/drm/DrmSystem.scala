package loamstream.drm

import loamstream.model.execute.EnvironmentType

/**
 * @author clint
 * May 23, 2018
 */
sealed trait DrmSystem

object DrmSystem {
  final case object Uger extends DrmSystem 
  final case object Lsf extends DrmSystem 
}
