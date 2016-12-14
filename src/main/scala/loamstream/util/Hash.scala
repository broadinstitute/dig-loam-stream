package loamstream.util

import scala.util.Try
import javax.xml.bind.DatatypeConverter

import scala.collection.mutable

/**
 * @author clint
 * date: Jul 28, 2016
 */
//NB: Use WrappedArray to ensure value field is compared by-value 
final case class Hash(value: mutable.WrappedArray[Byte], tpe: HashType) {
  //TODO: Stub
  
  override def toString: String = s"$tpe($valueAsHexString)"
  
  def valueAsHexString: String = DatatypeConverter.printHexBinary(value.toArray).toLowerCase
}

object Hash {
  def fromStrings(value: String, tpe: String): Try[Hash] = {
    for {
      bytes <- Try(DatatypeConverter.parseHexBinary(value))
      hashType <- HashType.fromAlgorithmName(tpe)
    } yield Hash(bytes, hashType)
  }
}