package loamstream.cli

import loamstream.model.jobs.Execution
import java.nio.file.Path
import loamstream.db.LoamDao
import loamstream.model.jobs.OutputRecord
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
      case Left(path) => OutputRecord(path)
      case Right(uri) => OutputRecord(uri)
    }
    
    doForOutput(dao)(outputRecord)
  }
  
  /**
   * Produce an Option wrapping a description of the Execution that produced this output, or None
   * if no such Execution is found. 
   */
  private def doForOutput(dao: LoamDao)(output: OutputRecord): Option[String] = {
    val executionOpt = dao.findExecution(output)
    
    executionOpt.map(describe)
  }
  
  private def describe(execution: Execution): String = {
    val commandString = execution.cmd.map(c => s"'$c'")getOrElse("<None>")
  
    val envType = execution.env.tpe
    val envSettings = execution.env.settings
  execution.result.get
    
    val stdout = execution.outputStreams.map(_.stdout).getOrElse("<Not produced>")
    val stderr = execution.outputStreams.map(_.stderr).getOrElse("<Not produced>")
  
    val numOutputs = execution.outputs.size
  
    //NB: Use '#' as a margin, to hopefully bypass any issues with commands including `|`.
    val withoutOutputs = s"""#
                             #Command: $commandString
                             #Environment: $envType
                             #Settings: $envSettings
                             #Output streams:
                             #  Stdout: $stdout
                             #  Stderr: $stderr
                             #Recorded information about $numOutputs output(s) follows:""".stripMargin('#')
      
    def outputRecordToString(o: OutputRecord): String = {
      val hashString = (for {
        ht <- o.hashType
        h <- o.hash
      } yield s"$ht: $h").getOrElse("None")
      
      s"""|  ${o.loc}
          |    Present? ${if(o.isPresent) "Yes" else "No"}
          |    Last modified: ${o.lastModified}
          |    Hash: $hashString""".stripMargin
    }
                             
    val outputs = execution.outputs.map(outputRecordToString).mkString("\n  ")

    s"${withoutOutputs}\n  ${outputs}"
  }
}
