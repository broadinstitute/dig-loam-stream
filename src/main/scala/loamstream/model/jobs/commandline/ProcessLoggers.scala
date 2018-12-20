package loamstream.model.jobs.commandline

import loamstream.conf.ExecutionConfig
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LogFileNames

/**
 * @author clint
 * Nov 13, 2017
 */
object ProcessLoggers {
  def forNamedJob(executionConfig: ExecutionConfig, job: LJob): ToFilesProcessLogger = {
    import executionConfig.jobOutputDir
    
    ToFilesProcessLogger(LogFileNames.outputStreams(job, jobOutputDir))
  }
}
