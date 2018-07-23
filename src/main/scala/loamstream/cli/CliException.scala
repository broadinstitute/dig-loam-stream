package loamstream.cli

/**
 * @author clint
 * Oct 19, 2016
 */
final case class CliException(msg: String, cause: Throwable) extends Exception(msg, cause) {
  def this(message: String) = this(message, None.orNull) //appease scalastyle :\
  def this(cause: Throwable) = this(cause.getMessage, cause)
}
