package loamstream.util

/**
 * @author clint
 * Mar 30, 2017
 */
final case class ExitCodeException(exitCode: Int, message: String) extends RuntimeException(message)
