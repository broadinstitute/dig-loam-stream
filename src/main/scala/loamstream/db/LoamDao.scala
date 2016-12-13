package loamstream.db

import loamstream.model.jobs.{Execution, OutputRecord}

/**
 * @author clint
 * date: Aug 4, 2016
 */
trait LoamDao {
  
  def findOutputRecord(loc: String): Option[OutputRecord]

  final def deleteOutput(loc: String, others: String*): Unit = deleteOutput(loc +: others)
  def deleteOutput(locs: Iterable[String]): Unit
  
  def allOutputRecords: Seq[OutputRecord]

  final def insertExecutions(execution: Execution, others: Execution*): Unit = insertExecutions(execution +: others)
  
  def insertExecutions(rows: Iterable[Execution]): Unit
  
  def allExecutions: Seq[Execution]
  
  def findExecution(output: OutputRecord): Option[Execution]
  
  def createTables(): Unit
  
  def dropTables(): Unit
  
  def shutdown(): Unit
}