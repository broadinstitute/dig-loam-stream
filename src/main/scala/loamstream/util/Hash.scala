package loamstream.util

import scala.util.Try
import javax.xml.bind.DatatypeConverter

import loamstream.util.HashType.{Md5, Sha1}

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
  def valueAsBase64String: String = DatatypeConverter.printBase64Binary(value.toArray).toLowerCase
}

object Hash {
  def fromStrings(value: Option[String], tpe: String): Try[Hash] = {
    for {
      bytes <- tpe match {
        case _ if tpe == Md5.algorithmName => Try(DatatypeConverter.parseBase64Binary(value.get))
        case _ if tpe == Sha1.algorithmName => Try(DatatypeConverter.parseHexBinary(value.get))
        case _ => Tries.failure(s"Unknown hash algorithm '$tpe'")
      }
      hashType <- HashType.fromAlgorithmName(tpe)
    } yield Hash(bytes, hashType)
  }
}
