package loamstream.conf

import loamstream.cli.Conf
import loamstream.util.jvm.JvmArgs
import loamstream.util.Options


/**
 * @author clint
 * Aug 13, 2020
 * 
 * JVM and application-level command-line settings used to invoke the current instance of LS.
 * Used to derive settings for worker LS instances run on DRM systems.   
 */
final case class LsSettings (
    cliConfig: Option[Conf.Values], 
    private val _jvmArgs: () => JvmArgs = () => LsSettings.defaultJvmArgs) {
  
  lazy val jvmArgs: JvmArgs = _jvmArgs()
  
  def thisInstanceIsAWorker: Boolean = {
    import Options.Implicits._
    
    cliConfig.map(_.workerSupplied).orElseFalse 
  }
}

object LsSettings {
  private def defaultJvmArgs = JvmArgs()
  
  val noCliConfig: LsSettings = new LsSettings(None, () => defaultJvmArgs)
  
  def apply(cliConfig: Conf.Values, jvmArgs: => JvmArgs): LsSettings = {
    new LsSettings(Option(cliConfig), () => jvmArgs)
  }
}
