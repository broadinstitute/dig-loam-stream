package loamstream.util

import javax.xml.bind.DatatypeConverter

import scala.collection.mutable

/**
 * @author clint
 * date: Jul 28, 2016
 */
//NB: Use WrappedArray to ensure value field is compared by-value 
final case class Hash(value: mutable.WrappedArray[Byte], tpe: HashType) {
  //TODO: Stub
  
  override def toString: String = s"$tpe($valueAsBase64String)"
  
  def valueAsBase64String: String = DatatypeConverter.printBase64Binary(value.toArray)
}

object Hash {
  def apply(value: String, tpe: HashType): Hash = {
    val bytes = DatatypeConverter.parseBase64Binary(value)
    
    Hash(bytes, tpe)
  }
  
  def fromStrings(value: Option[String], tpe: String): Option[Hash] = {
    for {
      bytes <- value.map(DatatypeConverter.parseBase64Binary)
      hashType <- HashType.fromAlgorithmName(tpe)
    } yield Hash(bytes, hashType)
  }
}
