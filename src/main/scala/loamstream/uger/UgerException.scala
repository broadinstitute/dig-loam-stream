package loamstream.uger

/**
 * @author clint
 * Mar 15, 2017
 */
final class UgerException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(cause: Throwable) = this(null, cause) //scalastyle.ignore:null
}

object UgerException extends (String => UgerException) {
  def apply(message: String): UgerException = new UgerException(message, null) //scalastyle.ignore:null
  
}
