package loamstream.db

import java.nio.file.Path
import loamstream.util.Hash

/**
 * @author clint
 * date: Aug 4, 2016
 */
trait LoamDao {
  def hashFor(path: Path): Hash
  
  def storeHash(path: Path, hash: Hash): Unit
}