package loamstream.db.slick


/**
 * @author clint
 * Apr 22, 2020
 */
trait CommandDaoOps { self: CommonDaoOps with OutputDaoOps with ExecutionDaoOps =>
  import driver.api._
  
  override def findCommand(loc: String): Option[String] = {
    for {
      outputRow <- findOutputRow(loc)
      executionId <- outputRow.executionId
      executionRow <- findExecutionRow(executionId)
      commandLine <- executionRow.cmd
    } yield commandLine
  }
}
