package loamstream.model.jobs

import java.nio.file.Path
import loamstream.util.Paths

/**
 * @author clint
 * Nov 13, 2017
 */
object LogFileNames {
  
  def outputStreams(job: LJob, outputDirName: Path): OutputStreams = {
    OutputStreams(stdout(job, outputDirName), stderr(job, outputDirName))
  }
  
  def stdout(job: LJob, outputDir: Path): Path = makePath(job, "stdout", outputDir)
  
  def stderr(job: LJob, outputDir: Path): Path = makePath(job, "stderr", outputDir)
  
  private def makePath(job: LJob, suffix: String, outputDir: Path): Path = {
    import Paths.Implicits._
    
    (outputDir / s"${mungeSpecialChars(job.name)}.$suffix").toAbsolutePath
  }
  
  //NB: Basically anything path-separator-related
  private[this] val specialChars: Set[Char] = Set('/', ':', '\\', '$')
  
  private def mungeSpecialChars(s: String): String = s.map {
    case ch if ch.isWhitespace || specialChars.contains(ch) => '_'
    case ch => ch      
  }
}
