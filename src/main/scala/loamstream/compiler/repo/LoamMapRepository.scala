package loamstream.compiler.repo

import loamstream.compiler.messages.{LoadResponseMessage, SaveResponseMessage}
import loamstream.util.{Hit, Shot, Snag}

/** A repository storing Loam scripts in memory as a Map */
case class LoamMapRepository(var entries: Map[String, String]) extends LoamRepository.Mutable {
  override def list: Seq[String] = entries.keys.toSeq

  override def load(name: String): Shot[LoadResponseMessage] =
    Shot.fromOption(entries.get(name).map(content => LoadResponseMessage(name, content, s"Got '$name' from memory.")),
      Snag("No entry for '$name'."))

  override def save(name: String, content: String): Shot[SaveResponseMessage] = {
    entries += (name -> content)
    Hit(SaveResponseMessage(name, s"Added '$name' to memory."))
  }
}
