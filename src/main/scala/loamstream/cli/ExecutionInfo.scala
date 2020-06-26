package loamstream.cli

import loamstream.model.jobs.Execution
import java.nio.file.Path
import loamstream.db.LoamDao
import loamstream.model.jobs.StoreRecord
import java.net.URI

/**
 * @author clint
 * Dec 11, 2017
 */
object ExecutionInfo {
  
  /**
   * Produce an Option wrapping a description of the Execution that produced this output, or None
   * if no such Execution is found. 
   */
  def forOutput(dao: LoamDao)(pathOrUri: Either[Path, URI]): Option[String] = {
    val outputRecord = pathOrUri match {
      case Left(path) => StoreRecord(path)
      case Right(uri) => StoreRecord(uri)
    }
    
    doForOutput(dao)(outputRecord)
  }
  
  /**
   * Produce an Option wrapping a description of the Execution that produced this output, or None
   * if no such Execution is found. 
   */
  private def doForOutput(dao: LoamDao)(output: StoreRecord): Option[String] = {
    val executionOpt = dao.findExecution(output)
    
    executionOpt.map(describe)
  }
  
  private def describe(execution: Execution.Persisted): String = {
    val none = "<None>"
    
    def toStringOrElse[A](opt: Option[A], default: String = none): String = {
      opt.map(_.toString).getOrElse(default)
    }
    
    val commandString = execution.cmd.map(c => s"'$c'").getOrElse(none)
  
    val envType = execution.envType
    
    val jobDir = toStringOrElse(execution.jobDir, "<Not produced>")
  
    val numOutputs = execution.outputs.size
  
    val termReasonPart = toStringOrElse(execution.terminationReason)
    
    //NB: Use '#' as a margin, to hopefully bypass any issues with commands including `|`.
    val withoutOutputs = s"""#
                             #Command: ${commandString}
                             #Environment: ${envType}
                             #Final status: ${execution.status}
                             #Job dir: ${jobDir}
                             #Termination reason: ${termReasonPart}
                             #Recorded information about $numOutputs output(s) follows:""".stripMargin('#')
      
    def outputRecordToString(o: StoreRecord): String = {
      s"""|  ${o.loc}
          |    Present? ${if(o.isPresent) "Yes" else "No"}
          |    Last modified: ${o.lastModified}""".stripMargin
    }
                             
    val outputs = execution.outputs.map(outputRecordToString).mkString(s"${System.lineSeparator}  ")

    s"${withoutOutputs}${System.lineSeparator}  ${outputs}"
  }
}
