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
  
  protected[slick] def findLastRunId: Option[Int] = findLastRunRow.map(_.id)
  
  override def findLastRun: Option[Run] = findLastRunRow.map(_.toRun)
  
  private def findLastRunRow: Option[RunRow] = {
    val action = RunQueries.newestRun.result.headOption
    
    log(action)
    
    runBlocking(action.transactionally)
  }
  
  override def registerNewRun(run: Run): Unit = {
    def deleteAction(numRuns: Int): Seq[DBIO[Int]] = numRuns match {
      case 0 | 1 => Nil
      case _ => {
        //Oh Slick... :\
        val query = tables.runs.filter(_.id in RunQueries.oldestRun.map(_.id)).delete
        
        log(query)
        
        Seq(query)
      }
    }
    
    import Implicits._
    
    val actions = for {
      numRuns <- numRunsIO
      _ = require(numRuns <= 2, s"Expected at most two Runs, but found $numRuns")
      deleteActions = deleteAction(numRuns)
      insertAction = RunQueries.insertRun += toRunRow(run)
      allActions = deleteActions :+ insertAction
      result <- DBIO.seq(allActions: _*).transactionally
    } yield {
      result
    }
    
    runBlocking(actions)
  }
  
  private def toRunRow(run: Run): RunRow = RunRow(DbHelpers.dummyId, run.name, Timestamp.valueOf(run.time))
  
  private def numRunsIO: DBIO[Int] = RunQueries.countRuns.result.transactionally
  
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
  }
}
