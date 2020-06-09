package loamstream.util

/**
 * @author clint
 * date: Jun 4, 2020
 */
object Base64 {
  def encode(data: Array[Byte]): String = org.apache.commons.codec.binary.Base64.encodeBase64String(data)

  def decode(s: String): Array[Byte] = org.apache.commons.codec.binary.Base64.decodeBase64(s)
}
