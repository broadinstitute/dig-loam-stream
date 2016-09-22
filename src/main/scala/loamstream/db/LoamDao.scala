package loamstream.db

import java.nio.file.Path

import loamstream.model.jobs.Output.CachedOutput
import loamstream.util.Hash
import loamstream.model.jobs.Execution
import loamstream.model.jobs.Output

/**
 * @author clint
 * date: Aug 4, 2016
 */
trait LoamDao {
  
  final def deleteOutput(path: Path, others: Path*): Unit = deleteOutput(path +: others)
  
  def deleteOutput(paths: Iterable[Path]): Unit
  
  //TODO: Need to allow updating?
  def insertOrUpdateOutput(rows: Iterable[CachedOutput]): Unit
  
  def allOutputRows: Seq[CachedOutput]
  
  def insertExecutions(rows: Iterable[Execution]): Unit
  
  def allExecutionRows: Seq[Execution]
  
  def createTables(): Unit
  
  def dropTables(): Unit
  
  def shutdown(): Unit
}