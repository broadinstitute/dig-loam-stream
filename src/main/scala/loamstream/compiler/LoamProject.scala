package loamstream.compiler

import loamstream.loam.LoamScript

/** A collection of Loam scripts to be compiled together */
object LoamProject {
  /** Returns LoamProject with one or more scripts */
  def apply(script: LoamScript, scripts: LoamScript*): LoamProject = LoamProject((script +: scripts).toSet)
  /** Returns LoamProject with only one script*/
  def apply(script: LoamScript): LoamProject = LoamProject(Set(script))
}

/** A collection of Loam scripts to be compiled together */
case class LoamProject(scripts: Set[LoamScript]) {

  /** Returns project with that script added */
  def +(script: LoamScript): LoamProject = copy(scripts = scripts + script)

  /** Returns project with those scripts added */
  def +(oScripts: Iterable[LoamScript]): LoamProject = copy(scripts = scripts ++ oScripts)

  /** Returns project with scripts of that project added */
  def ++(oProject: LoamProject): LoamProject = copy(scripts = scripts ++ oProject.scripts)

}
