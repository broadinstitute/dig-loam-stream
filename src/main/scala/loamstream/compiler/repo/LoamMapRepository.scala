package loamstream.compiler.repo

import loamstream.util.{Hit, Shot, Snag}

/**
  * LoamStream
  * Created by oliverr on 6/27/2016.
  */
case class LoamMapRepository(var entries: Map[String, String]) extends LoamRepository {
  override def list: Seq[String] = entries.keys.toSeq

  override def get(name: String): Shot[String] = Shot.fromOption(entries.get(name), Snag("No entry for '$name'."))

  override def add(name: String, content: String): Shot[String] = {
    entries += (name -> content)
    Hit(name)
  }
}
