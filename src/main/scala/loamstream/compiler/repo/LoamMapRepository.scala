package loamstream.compiler.repo

import loamstream.loam.LoamScript
import loamstream.util.{Hit, Shot, Snag}

/** A repository storing Loam scripts in memory as a Map */
final case class LoamMapRepository(private var _entries: Map[String, LoamScript]) extends LoamRepository.Mutable {
  private[this] val lock = new AnyRef

  def entries: Map[String, LoamScript] = lock.synchronized(_entries)

  /** Human-readable description on where the scripts are stored */
  override def description: String = "in memory"

  override def list: Seq[String] = entries.keys.toSeq

  override def save(script: LoamScript): Shot[LoamScript] = {
    lock.synchronized {
      _entries += (script.name -> script)
    }
    Hit(script)
  }

  /** Loads Loam script of given name from repository  */
  override def load(name: String): Shot[LoamScript] = {
    val scriptOpt = entries.get(name)
    Shot.fromOption(scriptOpt, Snag("No entry for '$name'."))
  }
}

object LoamMapRepository {
  /** Creates a LoamMapRepository from a collection of LoamScripts */
  def apply(scripts: Iterable[LoamScript]): LoamMapRepository =
  LoamMapRepository(scripts.map(script => (script.name, script)).toMap)
}
