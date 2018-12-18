package loamstream.conf

import java.nio.file.Paths
import java.nio.file.Path

/**
 * @author clint
 * Dec 17, 2018
 * 
 * An object containing the location of various things: output files, default log dir, etc.
 */
object Locations {
  val loamstreamDir = Paths.get("./.loamstream")
  
  val logDir: Path = loamstreamDir.resolve("logs").normalize
    
  val dryRunOutputFile: Path = logDir.resolve("joblist").normalize
    
  val jobOutputDir: Path = loamstreamDir.resolve("job-outputs").normalize
  
  val dbDir: Path = loamstreamDir.resolve("db")
  
  val ugerDir = loamstreamDir.resolve("uger").normalize
  val lsfDir = loamstreamDir.resolve("lsf").normalize
  
  val ugerScriptDir: Path = ugerDir.resolve("scripts").normalize
  val lsfScriptDir: Path = lsfDir.resolve("scripts").normalize
}
