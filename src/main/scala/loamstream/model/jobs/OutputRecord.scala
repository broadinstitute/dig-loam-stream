package loamstream.model.jobs

import java.nio.file.Path
import java.time.Instant

import loamstream.util.{PathUtils, TimeEnrichments}

/**
 * @author kyuksel
 * date: Dec 12, 2016
 *
 * A container for job output attributes that are to be recorded and are not system-dependent
 * (e.g. in hash type or how resources are identified [URI/Path/etc])
 */
final case class OutputRecord(loc: String,
                              isPresent: Boolean,
                              hash: Option[String],
                              lastModified: Option[Instant]) {

  def isMissing: Boolean = !isPresent

  def isOlderThan(other: OutputRecord): Boolean = {
    import TimeEnrichments.Implicits._

    lastModified match {
      case Some(timestamp) => other.lastModified match {
        case Some(otherTimestamp) => timestamp < otherTimestamp
        case None => false
      }
      case None => false
    }
  }

  def isHashed: Boolean = hash.isDefined

  def hasDifferentHashThan(other: OutputRecord): Boolean =
    (for {
        hashValue <- hash
        otherHashValue <- other.hash
      } yield hashValue != otherHashValue
    ).getOrElse(false)
}

object OutputRecord {
  def apply(loc: String,
            hash: Option[String],
            lastModified: Option[Instant]): OutputRecord = lastModified match {
    case Some(_) => OutputRecord(loc, isPresent = true, hash, lastModified)
    case _ => OutputRecord(loc, isPresent = false, hash, lastModified)
  }

  def apply(loc: String): OutputRecord = OutputRecord(loc,
                                                      isPresent = false,
                                                      hash = None,
                                                      lastModified = None)

  def apply(path: Path): OutputRecord = OutputRecord(PathUtils.normalize(path))

  def apply(output: Output): OutputRecord = OutputRecord( loc = output.location,
                                                          hash = output.hash.map(_.valueAsHexString),
                                                          lastModified = Option(output.lastModified))
}
