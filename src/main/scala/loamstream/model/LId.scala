package loamstream.model

import scala.util.Random

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
sealed trait LId

object LId {

  trait Owner {
    def id: LId
  }

  final case class LNamedId(name: String) extends LId

  final case class LAnonId(time: Long, random: Long) extends LId

  private val random = new Random

  def newAnonId: LAnonId = LAnonId(System.currentTimeMillis, random.nextLong())

}
