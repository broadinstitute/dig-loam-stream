package loamstream.model.jobs.commandline

import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LogFileNames
import java.nio.file.Path

/**
 * @author clint
 * Nov 13, 2017
 */
object ProcessLoggers {
  def forNamedJob(executionConfig: ExecutionConfig, job: LJob, jobOutputDir: Path): ToFilesProcessLogger = {
    ToFilesProcessLogger(LogFileNames.outputStreams(job, jobOutputDir))
  }
}
