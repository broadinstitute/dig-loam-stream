package loamstream.drm

import loamstream.model.execute.DrmSettings

/**
 * @author clint
 * May 11, 2018
 */
trait NativeSpecBuilder {
  def toNativeSpec(drmSettings: DrmSettings): String
}
