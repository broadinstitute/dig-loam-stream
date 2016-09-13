package loamstream.db

import java.nio.file.Path

import loamstream.model.jobs.Output.CachedOutput
import loamstream.util.Hash

/**
 * @author clint
 * date: Aug 4, 2016
 */
trait LoamDao {
  def hashFor(path: Path): Hash
  
  def storeHash(path: Path, hash: Hash): Unit
  
  final def delete(path: Path, others: Path*): Unit = delete(path +: others)
  
  def delete(paths: Iterable[Path]): Unit
  
  def insertOrUpdate(rows: Iterable[CachedOutput]): Unit 
  
  //TODO: Re-evaluate
  def allRows: Seq[CachedOutput]
 
  def createTables(): Unit
  
  def dropTables(): Unit
  
  def shutdown(): Unit
}