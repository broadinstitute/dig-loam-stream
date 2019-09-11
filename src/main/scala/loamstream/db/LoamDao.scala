package loamstream.db

import java.nio.file.Path

import loamstream.model.jobs.Execution
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.StoreRecord
import loamstream.util.Paths
import loamstream.model.jobs.JobStatus

/**
 * @author clint
 *         kyuksel
 * date: Aug 4, 2016
 */
trait LoamDao {
  final def insertExecutions(execution: Execution, others: Execution*): Unit = insertExecutions(execution +: others)
  def insertExecutions(rows: Iterable[Execution]): Unit

  def findExecution(outputLocation: String): Option[Execution]
  
  final def findExecution(output: StoreRecord): Option[Execution] = findExecution(output.loc)
  
  final def findLastStatus(outputLocation: String): Option[JobStatus] = findExecution(outputLocation).map(_.status)

  final def deleteOutput(loc: String, others: String*): Unit = deleteOutput(loc +: others)
  def deleteOutput(locs: Iterable[String]): Unit
  final def deletePathOutput(path: Path, others: Path*): Unit = deletePathOutput(path +: others)
  def deletePathOutput(path: Iterable[Path]): Unit

  def findStoreRecord(loc: String): Option[StoreRecord]
  final def findStoreRecord(path: Path): Option[StoreRecord] = findStoreRecord(Paths.normalize(path))
  final def findStoreRecord(rec: StoreRecord): Option[StoreRecord] = findStoreRecord(rec.loc)
  final def findStoreRecord(output: DataHandle): Option[StoreRecord] = findStoreRecord(output.toStoreRecord)

  /**
   * Returns the command line that was used to produce the output at the specified location
   */
  def findCommand(loc: String): Option[String]
  final def findCommand(path: Path): Option[String] = findCommand(Paths.normalize(path))

  def allStoreRecords: Seq[StoreRecord]
  
  def createTables(): Unit
  
  def dropTables(): Unit
  
  def shutdown(): Unit
}
