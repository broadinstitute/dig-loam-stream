package loamstream.model

import scala.util.Random

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
sealed trait LId { self =>
  def /(name: LId): LId.CompositeId = LId.CompositeId(self, name)
}

object LId {

  trait Owner {
    def id: LId
  }

  final case class LNamedId(name: String) extends LId {
    override def toString = name 
  }

  final case class LAnonId(time: Long, random: Long) extends LId
  
  final case class CompositeId(namespace: LId, name: LId) extends LId {
    override def toString = s"$namespace/$name"
  }
  
  case object Unknown extends LId

  private val random = new Random

  def newAnonId: LAnonId = LAnonId(System.currentTimeMillis, random.nextLong())

}
