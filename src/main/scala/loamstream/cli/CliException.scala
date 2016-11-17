package loamstream.cli

/**
 * @author clint
 * Oct 19, 2016
 */
final case class CliException(msg: String) extends Exception(msg)