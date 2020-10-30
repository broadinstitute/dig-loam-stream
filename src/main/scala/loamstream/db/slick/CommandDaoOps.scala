package loamstream.db.slick

import loamstream.util.Loggable


/**
 * @author clint
 * Apr 22, 2020
 */
trait CommandDaoOps { self: CommonDaoOps with OutputDaoOps with ExecutionDaoOps =>
  import driver.api._
  
  private object CommandDaoOpsLogger extends Loggable
  
  override def findCommand(loc: String): Option[String] = {
    for {
      outputRow <- findOutputRow(loc)
      _ = CommandDaoOpsLogger.trace(s"Looking up '$loc', found output row $outputRow")
      executionId <- outputRow.executionId
      _ = CommandDaoOpsLogger.trace(s"Looking up '$loc', found execution id $executionId")
      executionRow <- findExecutionRow(executionId)
      _ = CommandDaoOpsLogger.trace(s"Looking up '$loc', found execution row $executionRow")
      commandLine <- executionRow.cmd
    } yield commandLine
  }
}
