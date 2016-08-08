package loamstream.db.slick

import java.nio.file.Path
import loamstream.util.Hash
import loamstream.util.HashType
import java.nio.file.Paths

/**
 * @author clint
 * date: Aug 4, 2016
 */
final case class HashRow(pathValue: String, hashValue: String, hashType: String) {
  def this(path: Path, hash: Hash) = {
    this(path.toAbsolutePath.toString, hash.valueAsHexString, hash.tpe.algorithmName)
  }
  
  def toPath: Path = Paths.get(pathValue)
  
  def toHash: Hash = Hash.fromStrings(hashValue, hashType).get
}
