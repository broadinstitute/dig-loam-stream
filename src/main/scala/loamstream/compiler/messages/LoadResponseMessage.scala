package loamstream.compiler.messages

import loamstream.compiler.repo.LoamRepository
import loamstream.loam.LoamScript

/** Response to a load request, containing the Loam script to be loaded.
  *
  * @param name    Name of Loam script
  * @param content Content of Loam script
  * @param message A human-readable message typically describing where script was loaded from
  */
final case class LoadResponseMessage(name: String, content: String, message: String) extends ClientOutMessage {
  override val typeName: String = "load"
}

object LoadResponseMessage {
  def apply(repo: LoamRepository, script: LoamScript): LoadResponseMessage =
    LoadResponseMessage(script.name, script.code, s"Found ${script.name} ${repo.description}")
}