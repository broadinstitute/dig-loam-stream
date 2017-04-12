package loamstream.util

/**
 * @author clint
 * Mar 29, 2017
 */
final case class CompositeException(causes: Iterable[Throwable]) extends 
    RuntimeException(s"${causes.size} failures: ${causes.map(t => s"'$t'").mkString(";")}")
