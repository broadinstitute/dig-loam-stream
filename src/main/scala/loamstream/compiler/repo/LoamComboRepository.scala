package loamstream.compiler.repo

import loamstream.compiler.messages.{LoadResponseMessage, SaveResponseMessage}
import loamstream.util.Shot

/** A repository of Loam scripts combining two other repositories with saved scripts going to the first
  *
  * @param repo1 A mutable repository to which all save requests go
  * @param repo2 A repository to be read only
  */
final case class LoamComboRepository(repo1: LoamRepository.Mutable, repo2: LoamRepository) extends 
    LoamRepository.Mutable {
  
  /** Tries to save Loam script to repo1 */
  override def save(name: String, content: String): Shot[SaveResponseMessage] = repo1.save(name, content)

  /** Tries to load Loam script from repo1, if it fails from repo2 */
  override def load(name: String): Shot[LoadResponseMessage] = repo1.load(name).orElse(repo2.load(name))

  /** Returns combined list of names of available Loam scripts from repo1 and repo2 */
  override def list: Seq[String] = repo1.list ++ repo2.list
}
