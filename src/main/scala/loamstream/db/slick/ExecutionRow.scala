package loamstream.db.slick

import loamstream.model.execute.Environment
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
    terminationReasonType: Option[String],
    rawTerminationReason: Option[String]) {
  
  def toExecution(settings: Settings, resourcesOpt: Option[Resources], outputs: Set[StoreRecord]): Execution = {
    val commandResult = CommandResult(exitCode)

    val environmentOpt: Option[Environment] = for {
      tpe <- EnvironmentType.fromString(env)
      e <- Environment.from(tpe, settings)
    } yield e
    
    //TODO: :(
    require(environmentOpt.isDefined)
    
    import Paths.{get => toPath}
    
    val streamsOpt = Option(OutputStreams(toPath(stdoutPath), toPath(stderrPath)))

    val termReason = terminationReasonType.flatMap(TerminationReason.fromNameAndRawValue(_, rawTerminationReason))
    
    Execution(
        env = environmentOpt.get,
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
