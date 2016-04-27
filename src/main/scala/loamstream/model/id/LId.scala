package loamstream.model.id

import scala.util.Random

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
object LId {

  trait Owner {
    def id: LId
  }

  case class LNamedId(name: String) extends LId

  case class LAnonId(time: Long, random: Long) extends LId {
    override def name: String = time + "_" + random
  }

  val random = new Random

  def newAnonId: LAnonId = LAnonId(System.currentTimeMillis, random.nextLong())

  def fromName(name: String): LId = {
    if (name.matches("\\d+_\\d+")) {
      val tokens = name.split("_")
      val time = tokens(0).toLong
      val random = tokens(1).toLong
      LAnonId(time, random)
    } else {
      LNamedId(name)
    }
  }

}

sealed trait LId {
  def name: String
}
