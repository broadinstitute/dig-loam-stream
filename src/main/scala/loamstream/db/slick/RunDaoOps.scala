package loamstream.db.slick

import loamstream.db.LoamDao
import loamstream.model.execute.Run
import java.sql.Timestamp

/**
 * @author clint
 * Aug 25, 2020
 */
trait RunDaoOps extends LoamDao { self: CommonDaoOps =>
  import driver.api._
  
  //TODO: a bad idea
  override def findLastRunId: Option[Int] = {
    val action = RunQueries.newestRun.result.headOption
    
    log(action)
    
    runBlocking(action.transactionally).map(_.id)
  }
  
  override def findLastRun: Option[Run] = {
    assertAtMostTwoRuns()
    
    val action = RunQueries.newestRun.result.headOption
    
    log(action)
    
    runBlocking(action.transactionally).map(_.toRun)
  }
  
  override def registerNewRun(run: Run): Unit = {
    assertAtMostTwoRuns()

    val deleteActions: Seq[DBIO[Int]] = numRuns match {
      case 0 | 1 => Nil
      case _ => {
        //Oh Slick... :\
        val query = tables.runs.filter(_.id in RunQueries.oldestRun.map(_.id)).delete
        
        log(query)
        
        Seq(query)
      }
    }
    
    val insertAction: DBIO[_] = RunQueries.insertRun += toRunRow(run)
    
    val allActions: Seq[DBIO[_]] = deleteActions :+ insertAction
    
    runBlocking(DBIO.seq(allActions: _*).transactionally)
  }
  
  private def toRunRow(run: Run): RunRow = RunRow(DbHelpers.dummyId, run.name, Timestamp.valueOf(run.time))
  
  private def deleteOldestRun(): Unit = runBlocking(RunQueries.oldestRun.delete)
  
  private def deleteAllRuns(): Unit = runBlocking(RunQueries.allRuns.delete)
  
  private def numRuns: Int = runBlocking(RunQueries.countRuns.result.transactionally)
  
  private def assertAtMostTwoRuns(): Unit = {
    require(numRuns <= 2, s"Expected at most two Runs, but found $numRuns")
  }
  
  protected def findRunRow(runId: Int): Option[RunRow] = runBlocking(findRunAction(runId))
  
  private def findRunAction(runId: Int): DBIO[Option[RunRow]] = {
    RunQueries.runById(runId).result.headOption.transactionally
  }
  
  protected object RunQueries {
    val insertRun: driver.IntoInsertActionComposer[RunRow, RunRow] = {
      (tables.runs returning tables.runs.map(_.id)).into {
        (run, newId) => run.copy(id = newId)
      }
    }

    def allRuns: Query[tables.Runs, RunRow, Seq] = tables.runs
    
    def newestRun: Query[tables.Runs, RunRow, Seq] = allRuns.sortBy(_.timeStamp.desc).take(1)
    
    def oldestRun: Query[tables.Runs, RunRow, Seq] = allRuns.sortBy(_.timeStamp).take(1)
    
    val countRuns: Rep[Int] = tables.runs.length
    
    def runById(runId: Int): Query[tables.Runs, RunRow, Seq] = {
      tables.runs.filter(_.id === runId).take(1)
    }
  }
}
