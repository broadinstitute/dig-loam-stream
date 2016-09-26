package loamstream.db

import java.nio.file.Path

import loamstream.model.jobs.Output.CachedOutput
import loamstream.model.jobs.Output
import loamstream.util.Hash
import loamstream.model.jobs.Execution
import loamstream.model.jobs.Output

/**
 * @author clint
 * date: Aug 4, 2016
 */
trait LoamDao {
  
  def findOutput(path: Path): Option[CachedOutput]
  
  final def deleteOutput(path: Path, others: Path*): Unit = deleteOutput(path +: others)
  
  def deleteOutput(paths: Iterable[Path]): Unit
  
  final def insertOrUpdateOutputs(output: Output.PathBased, others: Output.PathBased*): Unit = {
    insertOrUpdateOutputs(output +: others)
  }
  
  def insertOrUpdateOutputs(rows: Iterable[Output.PathBased]): Unit
  
  def allOutputRows: Seq[CachedOutput]
  
  final def insertExecutions(execution: Execution, others: Execution*): Unit = insertExecutions(execution +: others)
  
  def insertExecutions(rows: Iterable[Execution]): Unit
  
  def allExecutions: Seq[Execution]
  
  def findExecution(output: Output.PathBased): Option[Execution]
  
  def createTables(): Unit
  
  def dropTables(): Unit
  
  def shutdown(): Unit
}