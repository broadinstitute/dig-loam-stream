package loamstream.compiler.repo

import loamstream.compiler.messages.{LoadResponseMessage, SaveResponseMessage}
import loamstream.util.Shot

/**
  * LoamStream
  * Created by oliverr on 6/28/2016.
  */
case class LoamComboRepository(repo1: LoamRepository.Mutable, repo2: LoamRepository) extends LoamRepository.Mutable {
  override def save(name: String, content: String): Shot[SaveResponseMessage] = repo1.save(name, content)

  override def load(name: String): Shot[LoadResponseMessage] = repo1.load(name).orElse(repo2.load(name))

  override def list: Seq[String] = repo1.list ++ repo2.list
}
