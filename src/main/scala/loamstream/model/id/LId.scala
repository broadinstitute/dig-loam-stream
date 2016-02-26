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

  case class LAnonId(time: Long, random: Long) extends LId

  val random = new Random

  def newAnonId = LAnonId(System.currentTimeMillis, random.nextLong())

}

sealed trait LId
