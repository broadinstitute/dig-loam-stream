package loamstream.util

import java.nio.file.Path
import scala.concurrent.duration.Duration

/**
 * @author clint
 * Jun 5, 2019
 */
final case class MissingFileTimeoutException(missingFile: Path, waitedFor: Duration) extends 
    Exception(s"Timed out after ${waitedFor} waiting for '${missingFile}' to appear")
