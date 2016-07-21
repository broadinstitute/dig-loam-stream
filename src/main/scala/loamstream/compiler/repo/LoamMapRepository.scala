package loamstream.compiler.repo

import loamstream.compiler.messages.{LoadResponseMessage, SaveResponseMessage}
import loamstream.util.{Hit, Shot, Snag}

/** A repository storing Loam scripts in memory as a Map */
final case class LoamMapRepository(private var _entries: Map[String, String]) extends LoamRepository.Mutable {
  private[this] val lock = new AnyRef
  
  def entries: Map[String, String] = lock.synchronized(_entries)
  
  override def list: Seq[String] = entries.keys.toSeq

  override def load(name: String): Shot[LoadResponseMessage] = {
    def toMessage(content: String) = LoadResponseMessage(name, content, s"Got '$name' from memory.")
    
    val messageOption = entries.get(name).map(toMessage)
    
    Shot.fromOption(messageOption, Snag("No entry for '$name'."))
  }

  override def save(name: String, content: String): Shot[SaveResponseMessage] = {
    lock.synchronized {
      _entries += (name -> content)
    }
    
    Hit(SaveResponseMessage(name, s"Added '$name' to memory."))
  }
}
