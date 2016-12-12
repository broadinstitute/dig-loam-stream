package loamstream.model.jobs

import loamstream.util.Hash
import java.time.Instant

/**
 * @author kyuksel
 * date: Dec 12, 2016
 * 
 * A container for job output attributes that are to be recorded and are not system-dependent
 */
final case class OutputRecord(isPresent: Boolean, hash: Hash, lastModified: Instant) {
  final def isMissing: Boolean = !isPresent
}