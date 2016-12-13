package loamstream.model.jobs

import java.time.Instant

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

  final def isMissing: Boolean = !isPresent

}

object OutputRecord {
  def apply(loc: String, hash: Option[String], lastModified: Option[Instant]): OutputRecord = lastModified match {
    case Some(_) => OutputRecord(loc, isPresent = true, hash, lastModified)
    case _ => OutputRecord(loc, isPresent = false, hash, lastModified)
  }
}