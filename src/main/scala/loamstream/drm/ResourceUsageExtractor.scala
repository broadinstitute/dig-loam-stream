package loamstream.drm

import scala.util.Try

import org.ggf.drmaa.JobInfo

import loamstream.model.execute.Resources.DrmResources

/**
 * @author clint
 * May 11, 2018
 */
trait ResourceUsageExtractor {
  def toResources(jobInfo: JobInfo): Try[DrmResources]
}
