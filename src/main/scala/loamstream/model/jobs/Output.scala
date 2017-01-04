package loamstream.model.jobs

import java.nio.file.Files
import java.nio.file.Path

import loamstream.util.{Hash, Hashes, PathUtils}
import java.time.Instant
import java.nio.file.Paths

/**
 * @author clint
 *         kaan
 * date: Aug 1, 2016
 * 
 * A trait representing a handle to the output of a job; for now, we're primarily concerned with the case where
 * that output is a file or directory, but other output types are possible.
 */
trait Output {
  def isPresent: Boolean
  
  final def isMissing: Boolean = !isPresent
  
  def hash: Option[Hash]

  def lastModified: Option[Instant]

  def location: String

  def toOutputRecord: OutputRecord
}

object Output {
  /**
   * A handle to an output of a job stored at 'path'.  Hashes and modification times are re-computed on
   * each access.
   */
  final case class PathOutput(path: Path) extends Output {
    override def hash: Option[Hash] = if (Files.exists(path)) Option(Hashes.sha1(path)) else None

    override def lastModified: Option[Instant] = {
      if (isPresent) Option(PathUtils.lastModifiedTime(path)) else None
    }

    override def isPresent: Boolean = Files.exists(path)

    override def location: String = PathUtils.normalize(path)

    override def toOutputRecord: OutputRecord = {
      OutputRecord(location, hash.map(_.valueAsHexString), lastModified)
    }

    def normalized: PathOutput = copy(path = normalize(path))

    private def normalize(p: Path): Path = Paths.get(PathUtils.normalize(p))
  }
}
