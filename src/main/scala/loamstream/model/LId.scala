package loamstream.model

import scala.util.Random
import loamstream.util.Sequence

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
sealed trait LId {
  def name: String
}

object LId {

  trait Owner {
    def id: LId
  }

  final case class LNamedId(name: String) extends LId {
    override def toString = name 
  }

  final case class LAnonId(id: Long) extends LId {
    require(id >= 0)
    
    override def name: String = "anon$" + id

    override def toString = name 
  }
  
  def newAnonId: LAnonId = LAnonId(ids.next())

  private val anonIdNameRegex = "anon\\$(\\d+)".r
  
  def fromName(name: String): LId = name match {
    case anonIdNameRegex(id) => LAnonId(id.toLong)
    case _ => LNamedId(name)
  }
  
  private val ids: Sequence[Long] = Sequence()
}
