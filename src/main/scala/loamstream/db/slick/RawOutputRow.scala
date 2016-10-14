package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Timestamp

import loamstream.model.jobs.Output
import loamstream.model.jobs.Output.CachedOutput
import loamstream.util.Hash
import loamstream.util.PathUtils.lastModifiedTime
import loamstream.util.PathUtils.normalize

/**
 * @author clint
 * date: Aug 4, 2016
 * 
 * A class representing a row in the 'Outputs' table. 
 */
final case class RawOutputRow private (
    pathValue: String, 
    lastModified: Timestamp, 
    hashValue: String, 
    hashType: String,
    executionId: Option[Int] = None) {
  
  def this(path: Path, hash: Hash) = {
    this(
        normalize(path), 
        Timestamp.from(lastModifiedTime(path)), 
        hash.valueAsHexString, 
        hash.tpe.algorithmName,
        None)
  }
  
  def this(output: Output.PathBased) = this(output.path, output.hash)
  
  def toPath: Path = Paths.get(pathValue)
  
  //NB: Fragile
  def toHash: Hash = Hash.fromStrings(hashValue, hashType).get
  
  def toCachedOutput: CachedOutput = CachedOutput(toPath, toHash, lastModified.toInstant)
  
  def withExecutionId(newExecutionId: Int): RawOutputRow = copy(executionId = Some(newExecutionId))
}
