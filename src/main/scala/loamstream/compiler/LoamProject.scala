package loamstream.compiler

import loamstream.loam.LoamScript
import loamstream.conf.LoamConfig
import loamstream.conf.LsSettings
import scala.collection.compat._

/** A collection of Loam scripts to be compiled together */
object LoamProject {
  /** Returns LoamProject with one or more scripts */
  def apply(config: LoamConfig, settings: LsSettings, script: LoamScript, scripts: LoamScript*): LoamProject = {
    LoamProject(config, settings, script +: scripts)
  }

  /** Returns LoamProject with iterable of scripts */
  def apply(config: LoamConfig, settings: LsSettings, scripts: Iterable[LoamScript]): LoamProject = {
    LoamProject(config, settings, scripts.to(Set))
  }
}

/** A collection of Loam scripts to be compiled together */
final case class LoamProject(config: LoamConfig, settings: LsSettings, scripts: Set[LoamScript]) {

  /** Returns project with that script added */
  def +(script: LoamScript): LoamProject = copy(scripts = scripts + script)

  /** Returns project with those scripts added */
  def +(oScripts: Iterable[LoamScript]): LoamProject = copy(scripts = scripts ++ oScripts)

  /** Returns project with scripts of that project added */
  def ++(oProject: LoamProject): LoamProject = copy(scripts = scripts ++ oProject.scripts)

}
