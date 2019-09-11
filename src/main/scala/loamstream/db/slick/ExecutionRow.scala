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
    cmd: Option[String], 
    status: JobStatus, 
    exitCode: Int,
    jobDir: Option[String],
    terminationReason: Option[String]) {
  
  def toExecution(resourcesOpt: Option[Resources], outputs: Set[StoreRecord]): Execution = {
    val commandResult = CommandResult(exitCode)

    import Paths.{get => toPath}
    
    val termReason = terminationReason.flatMap(TerminationReason.fromName)
    
    val envTypeOpt = EnvironmentType.fromString(env)
    
    require(envTypeOpt.isDefined, s"Unknown environment type name '${env}'")
    
    Execution(
        envType = envTypeOpt.get,
        settings = None,
        cmd = cmd,
        status = status,
        result = Option(commandResult),
        resources = resourcesOpt,
        outputs = outputs,
        jobDir = jobDir.map(toPath(_)),
        terminationReason = termReason)
  }
}
