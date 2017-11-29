package loamstream.util

/**
 * @author clint
 *         kaan
 * date: Aug 1, 2016
 */
sealed trait HashType {
  /**
   * An algorithm name suitable for passing to java.security.MessageDigest.getInstance
   */
  def algorithmName: String
  
  def isSha1: Boolean = this == HashType.Sha1
  def isMd5: Boolean = this == HashType.Md5
  def isSupported: Boolean = isSha1 || isMd5
}

object HashType {
  case object Sha1 extends HashType {
    override val algorithmName: String = "SHA-1"
  }

  case object Md5 extends HashType {
    override val algorithmName: String = "MD5"
  }

  def fromAlgorithmName(name: String): Option[HashType] = {
    if (name == Sha1.algorithmName) { Some(Sha1) }
    else if (name == Md5.algorithmName) { Some(Md5) }
    else { None }
  }
}
