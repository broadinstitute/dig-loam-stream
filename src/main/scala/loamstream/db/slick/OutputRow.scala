package loamstream.db.slick

import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Timestamp

import loamstream.model.jobs.Output
import loamstream.model.jobs.Output.CachedOutput
import loamstream.util.Hash
import loamstream.util.PathUtils.lastModifiedTime
import loamstream.util.PathUtils.normalize
import loamstream.util.Options
import loamstream.model.jobs.Output.PathOutput

/**
 * @author clint
 * date: Aug 4, 2016
 * 
 * A class representing a row in the 'Outputs' table. 
 */
final case class OutputRow private (
    pathValue: String, 
    lastModified: Option[Timestamp], 
    hashValue: Option[String], 
    hashType: Option[String],
    executionId: Option[Int] = None) {
  
  def this(path: Path) = {
    this(
        normalize(path), 
        None, 
        None, 
        None,
        None)
  }
  
  def this(path: Path, hash: Hash) = {
    this(
        normalize(path), 
        Option(Timestamp.from(lastModifiedTime(path))), 
        Option(hash.valueAsHexString), 
        Option(hash.tpe.algorithmName),
        None)
  }
  
  def this(output: Output.PathBased) = this(output.path, output.hash)
  
  def withExecutionId(newExecutionId: Int): OutputRow = copy(executionId = Some(newExecutionId))
  
  def toPath: Path = Paths.get(pathValue)
  
  def toPathOutput: PathOutput = PathOutput(toPath)
  
  def toOutput: Output.PathBased = toPathOutput
  
  def toCachedOutput: CachedOutput = {
    val hashAttempt = {
      import Options.toTry
      
      for {
        hv <- toTry(hashValue)("Hash value is missing")
        ht <- toTry(hashType)("Hash type is missing")
        hash <- Hash.fromStrings(hv, ht)
      } yield hash
    }
    
    //NB: Fragile
    val hash = hashAttempt.get
    //NB: Fragile
    val modified = lastModified.get 
    
    CachedOutput(toPath, hash, modified.toInstant)
  }
}
