package loamstream.util

import scala.collection.immutable.ArraySeq

/**
 * @author clint
 * date: Jul 28, 2016
 */
//NB: Use WrappedArray to ensure value field is compared by-value 
final case class Hash(value: ArraySeq[Byte], tpe: HashType) {
  //TODO: Stub
  
  override def toString: String = s"$tpe($valueAsBase64String)"
  
  def valueAsBase64String: String = Base64.encode(value.toArray)
}

object Hash {
  def fromStrings(value: Option[String], tpe: String): Option[Hash] = {
    for {
      bytes <- value.map(Base64.decode)
      hashType <- HashType.fromAlgorithmName(tpe)
    } yield Hash(ArraySeq.from(bytes), hashType)
  }
}
