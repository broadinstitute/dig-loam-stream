package loamstream.conf

import com.typesafe.config.Config
import scala.util.Try
import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author clint
 * Apr 20, 2017
 */
final case class ExecutionConfig(maxRunsPerJob: Int, outputDir: Path)

object ExecutionConfig extends ConfigParser[ExecutionConfig] {

  private val defaultOutputDir: Path = Paths.get("job-outputs")
  
  val default: ExecutionConfig = ExecutionConfig(
      maxRunsPerJob = 4, //scalastyle:ignore magic.number 
      outputDir = defaultOutputDir) 
  
  override def fromConfig(config: Config): Try[ExecutionConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader

    //NB: Ficus now marshals the contents of loamstream.execution into an ExecutionConfig instance.
    //Names of fields in ExecutionConfig and keys under loamstream.execution must match.
    Try(config.as[ExecutionConfig]("loamstream.execution"))
  }
}
