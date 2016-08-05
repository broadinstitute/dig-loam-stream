package loamstream.util

import javax.xml.bind.DatatypeConverter
import scala.util.Try

/**
 * @author clint
 * date: Jul 28, 2016
 */
final case class Hash(value: Array[Byte], tpe: HashType) {
  //TODO: Stub
  
  override def toString: String = s"$tpe($valueAsHexString)"
  
  def valueAsHexString: String = DatatypeConverter.printHexBinary(value).toLowerCase
}

object Hash {
  def fromStrings(value: String, tpe: String): Try[Hash] = {
    for {
      bytes <- Try(DatatypeConverter.parseHexBinary(value))
      hashType <- HashType.fromAlgorithmName(tpe)
    } yield Hash(bytes, hashType)
  }
}