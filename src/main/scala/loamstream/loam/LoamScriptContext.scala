package loamstream.loam

import java.nio.file.Path
import java.nio.file.Paths

import loamstream.conf.LoamConfig
import loamstream.conf.LsfConfig
import loamstream.conf.PythonConfig
import loamstream.conf.RConfig
import loamstream.conf.UgerConfig
import loamstream.googlecloud.GoogleCloudConfig
import loamstream.googlecloud.HailConfig
import loamstream.model.execute.LocalSettings
import loamstream.model.execute.Settings
import loamstream.util.ValueBox

/** Container for compile time and run time context for a script */
final class LoamScriptContext(val projectContext: LoamProjectContext) {

  val workDirBox: ValueBox[Path] = ValueBox(Paths.get("."))

  def workDir: Path = workDirBox.value

  def setWorkDir(newDir: Path): Path = {
    workDirBox.value = newDir
    newDir
  }

  def changeWorkDir(newDir: Path): Path = workDirBox.mutate(_.resolve(newDir)).value
  
  private val settingsBox: ValueBox[Settings] = ValueBox(LocalSettings)

  def settings: Settings = settingsBox.value

  def settings_=(newSettings: Settings): Unit = {
    settingsBox.value = newSettings
  }

  lazy val executionId: String = s"${java.util.UUID.randomUUID}"
  
  def config: LoamConfig = projectContext.config
  
  def ugerConfig: UgerConfig = {
    getOpt(config.ugerConfig, s"Uger support requires a valid 'loamstream.uger' section in the config file")
  }
  
  def lsfConfig: LsfConfig = {
    getOpt(config.lsfConfig, s"LSF support requires a valid 'loamstream.lsf' section in the config file")
  }
  
  def rConfig: RConfig = {
    getOpt(config.rConfig, s"R support requires a valid 'loamstream.r' section in the config file")
  }
  
  def pythonConfig: PythonConfig = {
    getOpt(config.pythonConfig, s"Python support requires a valid 'loamstream.python' section in the config file")
  }
  
  def hailConfig: HailConfig = {
    getOpt(
        config.hailConfig, 
        s"Hail support requires a valid 'loamstream.googlecloud.hail' section in the config file")
  }
  
  def googleConfig: GoogleCloudConfig = {
    getOpt(config.googleConfig, s"Google support requires a valid 'loamstream.googlecloud' section in the config file")
  }
  
  private def getOpt[A](opt: Option[A], missingMessage: => String): A = {
    require(opt.isDefined, missingMessage)
    
    opt.get
  }
}
