package loamstream.model.jobs

import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author clint
 * Nov 13, 2017
 */
object LogFileNames {
  
  private val defaultOutputDirName: String = "job-outputs"
  
  def stdout(job: LJob, outputDirName: String = defaultOutputDirName): Path = makePath(job, "stdout", outputDirName)
  
  def stderr(job: LJob, outputDirName: String = defaultOutputDirName): Path = makePath(job, "stderr", outputDirName)
  
  private def makePath(job: LJob, suffix: String, outputDirName: String): Path = {
    Paths.get(s"${outputDirName}/${mungeSpecialChars(job.name)}.$suffix").toAbsolutePath
  }
  
  //NB: Basically anything path-separator-related
  private[this] val specialChars: Set[Char] = Set('/', ':', '\\')
  
  private def mungeSpecialChars(s: String): String = s.map {
    case ch if ch.isWhitespace || specialChars.contains(ch) => '_'
    case ch => ch      
  }
}
