package loamstream.conf

import com.typesafe.config.Config
import scala.util.Try
import java.nio.file.Path
import java.nio.file.Paths
import CompilationConfig.Defaults
import scala.concurrent.duration.Duration

/**
 * @author clint
 * Apr 20, 2017
 */
final case class CompilationConfig(shouldValidateGraph: Boolean = Defaults.shouldValidateGraph)

object CompilationConfig extends ConfigParser[CompilationConfig] {

  object Defaults {
    val shouldValidateGraph: Boolean = true
  }
  
  val default: CompilationConfig = CompilationConfig()
  
  override def fromConfig(config: Config): Try[CompilationConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._

    //NB: Ficus now marshals the contents of loamstream.execution into an ExecutionConfig instance.
    //Names of fields in ExecutionConfig and keys under loamstream.execution must match.
    Try(config.as[CompilationConfig]("loamstream.compilation"))
  }
}
