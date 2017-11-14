package loamstream.model.jobs

import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author clint
 * Nov 13, 2017
 */
object LogFileNames {
  
  def stdout(job: LJob): Path = makePath(job, "stdout")
  
  def stderr(job: LJob): Path = makePath(job, "stderr")
  
  private val outputDirName = "job-outputs"
  
  private def makePath(job: LJob, suffix: String): Path = Paths.get(s"$outputDirName/${job.name}.$suffix")
}
