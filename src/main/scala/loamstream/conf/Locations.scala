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
  
  import java.nio.file.Files.createDirectories
  
  lazy val loamstreamDir: Path = createDirectories(Paths.get("./.loamstream"))
  
  lazy val logDir: Path = createDirectories(loamstreamDir.resolve("logs").normalize)
    
  val dryRunOutputFile: Path = logDir.resolve("joblist").normalize
    
  lazy val jobOutputDir: Path = createDirectories(loamstreamDir.resolve("job-outputs").normalize)
  
  lazy val dbDir: Path = createDirectories(loamstreamDir.resolve("db"))
  
  lazy val ugerDir = createDirectories(loamstreamDir.resolve("uger").normalize)
  lazy val lsfDir = createDirectories(loamstreamDir.resolve("lsf").normalize)
  
  lazy val ugerScriptDir: Path = createDirectories(ugerDir.resolve("scripts").normalize)
  lazy val lsfScriptDir: Path = createDirectories(lsfDir.resolve("scripts").normalize)
}
