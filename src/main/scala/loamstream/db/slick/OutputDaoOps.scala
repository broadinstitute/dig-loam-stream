package loamstream.db.slick

import java.nio.file.Path

import loamstream.db.LoamDao
import loamstream.model.jobs.StoreRecord
import loamstream.util.Paths
import slick.jdbc.JdbcProfile
import scala.collection.compat._

/**
 * @author clint
 * Dec 7, 2017
 * 
 * NB: Factored out of SlickLoamDao, which had gotten huge
 */
trait OutputDaoOps extends LoamDao { self: CommonDaoOps =>
  import driver.api._
  
  override def findStoreRecord(loc: String): Option[StoreRecord] = findOutputRow(loc).map(toOutputRecord)
  
  override def deleteOutput(locs: Iterable[String]): Unit = {
    val delete = outputDeleteAction(locs)

    runBlocking(delete.transactionally)
  }

  override def deletePathOutput(paths: Iterable[Path]): Unit = {
    deleteOutput(paths.map(Paths.normalize))
  }
  
  //TODO: Find way to extract common code from the all* methods
  override def allStoreRecords: Seq[StoreRecord] = {
    val query = tables.outputs.result

    log(query)

    runBlocking(query.transactionally).map(toOutputRecord)
  }
  
  private def doFindOutput[A](loc: String, f: OutputRow => A): Option[A] = { 
    val action = findOutputAction(loc)

    runBlocking(action).map(f)
  }
  
  private def findOutputAction(loc: String): DBIO[Option[OutputRow]] = {
    OutputQueries.outputByLoc(loc).result.headOption.transactionally
  }
  
  protected def findOutputRow(loc: String): Option[OutputRow] = {
    val action = findOutputAction(loc)

    runBlocking(action)
  }
  
  protected def toOutputRecord(row: OutputRow): StoreRecord = row.toStoreRecord
  
  protected def outputDeleteAction(locsToDelete: Iterable[String]): WriteAction[Int] = {
    OutputQueries.outputsByPaths(locsToDelete).delete
  }

  protected def outputDeleteActionRaw(pathsToDelete: Iterable[String]): WriteAction[Int] = {
    OutputQueries.outputsByRawPaths(pathsToDelete).delete
  }
  
  private[slick] def insertOrUpdateOutputRows(rows: Iterable[OutputRow]): DBIO[Iterable[Int]] = {
    val insertActions = rows.map(tables.outputs.insertOrUpdate)

    DBIO.sequence(insertActions).transactionally
  }
  
  private object OutputQueries {
    def outputByLoc(loc: String): Query[tables.Outputs, OutputRow, Seq] = {
      tables.outputs.filter(_.locator === loc).take(1)
    }

    def outputsByPaths(locs: Iterable[String]): Query[tables.Outputs, OutputRow, Seq] = {
      val rawPaths = locs.to(Set)

      outputsByRawPaths(rawPaths)
    }

    def outputsByRawPaths(rawPaths: Iterable[String]): Query[tables.Outputs, OutputRow, Seq] = {
      tables.outputs.filter(_.locator.inSetBind(rawPaths))
    }
  }
}
