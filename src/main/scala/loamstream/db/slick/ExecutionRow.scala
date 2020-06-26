package loamstream.db.slick

import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.Execution
import loamstream.model.execute.EnvironmentType
import java.nio.file.Paths
import loamstream.model.jobs.TerminationReason
import loamstream.model.jobs.JobResult

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
  
  def toExecution(outputRows: Iterable[OutputRow]): Execution.Persisted = {
    val envTypeOpt = EnvironmentType.fromString(env)
    
    require(envTypeOpt.isDefined, s"Unknown EnvironmentType encountered: '$env'")
    
    val jobDirPath = jobDir.map(Paths.get(_))
    
    val termReason = terminationReason.flatMap(TerminationReason.fromName)
    
    val outputs = outputRows.map(_.toStoreRecord).toSet
    
    //TODO
    val result = Option(JobResult.CommandResult(exitCode)) 
    
    Execution.Persisted(envTypeOpt.get, cmd, status, result, outputs, jobDirPath, termReason)
  }
}
