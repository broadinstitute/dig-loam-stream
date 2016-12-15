package loamstream.db

import java.nio.file.Path

import loamstream.model.jobs.{Execution, Output, OutputRecord}
import loamstream.util.PathUtils

/**
 * @author clint
 *         kyuksel
 * date: Aug 4, 2016
 */
trait LoamDao {
  
  def findOutputRecord(loc: String): Option[OutputRecord]
  final def findOutputRecord(path: Path): Option[OutputRecord] = findOutputRecord(PathUtils.normalize(path))
  final def findOutputRecord(rec: OutputRecord): Option[OutputRecord] = findOutputRecord(rec.loc)
  final def findOutputRecord(output: Output): Option[OutputRecord] = findOutputRecord(output.toOutputRecord)

  final def deleteOutput(loc: String, others: String*): Unit = deleteOutput(loc +: others)
  def deleteOutput(locs: Iterable[String]): Unit

  final def deletePathOutput(path: Path, others: Path*): Unit = deletePathOutput(path +: others)
  def deletePathOutput(path: Iterable[Path]): Unit
  
  def allOutputRecords: Seq[OutputRecord]

  final def insertExecutions(execution: Execution, others: Execution*): Unit = insertExecutions(execution +: others)
  
  def insertExecutions(rows: Iterable[Execution]): Unit
  
  def allExecutions: Seq[Execution]
  
  def findExecution(output: OutputRecord): Option[Execution]
  
  def createTables(): Unit
  
  def dropTables(): Unit
  
  def shutdown(): Unit
}
