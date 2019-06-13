package loamstream.util

import java.nio.file.Path
import loamstream.model.jobs.OutputStreams

/**
 * @author clint
 * Nov 13, 2017
 */
object LogFileNames {
  
  def outputStreams(outputDirName: Path): OutputStreams = {
    OutputStreams(stdout(outputDirName), stderr(outputDirName))
  }
  
  def stdout(outputDir: Path): Path = makePath(outputDir, "stdout")
  
  def stderr(outputDir: Path): Path = makePath(outputDir, "stderr")
  
  private def makePath(outputDir: Path, fileName: String): Path = {
    import Paths.Implicits._
    
    (outputDir / fileName).toAbsolutePath
  }
}
