package loamstream.model.id

import loamstream.util.shot.{Hit, Miss, Shot}

import scala.util.Random

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
object LId {

  trait Owner {
    def ownerBaseName: String

    def id: LId

    def nameFromId: String = id.asName(ownerBaseName)
  }

  case class LNamedId(name: String) extends LId {
    override def asName(baseName: String): String = baseName + name
  }

  case class LAnonId(time: Long, random: Long) extends LId {
    override def asName(baseName: String): String = baseName + time + "_" + random
  }

  val random = new Random

  def newAnonId: LAnonId = LAnonId(System.currentTimeMillis, random.nextLong())

  def fromName(name: String, baseName: String): Shot[LId] = {
    if (name.startsWith(baseName)) {
      val suffix = name.replaceFirst(baseName, "")
      if (suffix.matches("\\d+_\\d+")) {
        val tokens = suffix.split("_")
        val time = tokens(0).toLong
        val random = tokens(1).toLong
        Hit(LAnonId(time, random))
      } else {
        Hit(LNamedId(suffix))
      }
    } else {
      Miss(s"Name '$name' needs to start with '$baseName' but doesn't.")
    }
  }

}

sealed trait LId {
  def asName(baseName: String): String
}
