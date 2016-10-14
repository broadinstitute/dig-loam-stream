package loamstream.compiler.messages

import loamstream.compiler.repo.LoamRepository
import loamstream.loam.LoamScript

/** A message sent to client that a Loam script has been successfully saved
  *
  * @param name    Name of Loam script
  * @param message Message about the saving such as where it was saved to
  */
final case class SaveResponseMessage(name: String, message: String) extends ClientOutMessage {
  override val typeName: String = "save"
}

object SaveResponseMessage {
  def apply(repo: LoamRepository, script: LoamScript): SaveResponseMessage =
    SaveResponseMessage(script.name, s"Stores ${script.name} ${repo.description}")
}
