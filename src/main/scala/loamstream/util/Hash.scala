package loamstream.util

import javax.xml.bind.DatatypeConverter

/**
 * @author clint
 * date: Jul 28, 2016
 */
final case class Hash(value: Array[Byte], tpe: HashType) {
  //TODO: Stub
  
  override def toString: String = s"$tpe($valueAsHexString)"
  
  def valueAsHexString: String = DatatypeConverter.printHexBinary(value).toLowerCase
}