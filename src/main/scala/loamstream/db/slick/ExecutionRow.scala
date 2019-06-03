package loamstream.db.slick

import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.Resources
import loamstream.model.execute.Settings
import loamstream.model.jobs.Execution
import loamstream.model.jobs.JobResult.CommandResult
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.StoreRecord
import java.nio.file.Paths
import loamstream.model.jobs.OutputStreams
import loamstream.model.jobs.TerminationReason

/**
 * @author clint
 *         date: Sep 22, 2016
 */
final case class ExecutionRow(
    id: Int, 
    env: String, 
    cmd: String, 
    status: JobStatus, 
    exitCode: Int,
    stdoutPath: String,
    stderrPath: String,
    terminationReason: Option[String]) {
  
  def toExecution(settings: Settings, resourcesOpt: Option[Resources], outputs: Set[StoreRecord]): Execution = {
    val commandResult = CommandResult(exitCode)

    
    import Paths.{get => toPath}
    
    val streamsOpt = Option(OutputStreams(toPath(stdoutPath), toPath(stderrPath)))

    val termReason = terminationReason.flatMap(TerminationReason.fromName)
    
    Execution(
        settings = settings,
        cmd = Option(cmd),
        status = status,
        result = Option(commandResult),
        resources = resourcesOpt,
        outputs = outputs,
        outputStreams = streamsOpt,
        terminationReason = termReason)
  }
}
