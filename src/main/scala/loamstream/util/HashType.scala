package loamstream.util

/**
 * @author clint
 * date: Aug 1, 2016
 */
trait HashType {
  /**
   * An algorithm name suitable for passing to java.security.MessageDigest.getInstance
   */
  def algorithmName: String
  
  def isSha1: Boolean = this == HashType.Sha1 
}

object HashType {
  case object Sha1 extends HashType {
    override val algorithmName: String = "SHA-1"
  }
}