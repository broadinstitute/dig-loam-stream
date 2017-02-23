package loamstream.compiler

import loamstream.loam.LoamScript
import loamstream.conf.LoamConfig

/** A collection of Loam scripts to be compiled together */
object LoamProject {
  /** Returns LoamProject with one or more scripts */
  def apply(config: LoamConfig, script: LoamScript, scripts: LoamScript*): LoamProject = {
    LoamProject(config, script +: scripts)
  }

  /** Returns LoamProject with iterable of scripts */
  def apply(config: LoamConfig, scripts: Iterable[LoamScript]): LoamProject = LoamProject(config, scripts.toSet)

  /** Returns LoamProject with only one script */
  def apply(config: LoamConfig, script: LoamScript): LoamProject = LoamProject(config, Set(script))
}

/** A collection of Loam scripts to be compiled together */
final case class LoamProject(config: LoamConfig, scripts: Set[LoamScript]) {

  /** Returns project with that script added */
  def +(script: LoamScript): LoamProject = copy(scripts = scripts + script)

  /** Returns project with those scripts added */
  def +(oScripts: Iterable[LoamScript]): LoamProject = copy(scripts = scripts ++ oScripts)

  /** Returns project with scripts of that project added */
  def ++(oProject: LoamProject): LoamProject = copy(scripts = scripts ++ oProject.scripts)

}
