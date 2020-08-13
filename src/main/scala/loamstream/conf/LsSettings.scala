package loamstream.conf

import loamstream.cli.Conf
import loamstream.util.jvm.JvmArgs
import loamstream.util.Options


/**
 * @author clint
 * Aug 13, 2020
 */
final case class LsSettings(cliConfig: Option[Conf.Values]) {
  lazy val jvmArgs: JvmArgs = JvmArgs()
  
  def thisInstanceIsAWorker: Boolean = {
    import Options.Implicits._
    
    cliConfig.map(_.workerSupplied).orElseFalse 
  }
}

object LsSettings {
  val noCliConfig: LsSettings = LsSettings(None)
}
