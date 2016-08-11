package loamstream.db.slick

import java.nio.file.Path
import loamstream.util.Hash
import loamstream.util.HashType
import java.nio.file.Paths
import java.sql.Timestamp
import loamstream.db.OutputRow
import java.time.Instant
import Helpers._
import loamstream.util.PathUtils.lastModifiedTime

/**
 * @author clint
 * date: Aug 4, 2016
 */
final case class RawOutputRow(pathValue: String, lastModified: Timestamp, hashValue: String, hashType: String) {
  def this(path: Path, hash: Hash) = {
    this(
        normalize(path), 
        Timestamp.from(lastModifiedTime(path)), 
        hash.valueAsHexString, 
        hash.tpe.algorithmName)
  }
  
  def toPath: Path = Paths.get(pathValue)
  
  def toHash: Hash = Hash.fromStrings(hashValue, hashType).get
  
  def toOutputRow: OutputRow = OutputRow(toPath, lastModified.toInstant, toHash)
}
