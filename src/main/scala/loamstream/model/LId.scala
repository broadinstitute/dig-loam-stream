package loamstream.model

import loamstream.util.Sequence

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
sealed trait LId extends Any {
  def name: String
}

object LId {

  trait HasId extends java.lang.Comparable[HasId] {
    def id: LId
    
    override def compareTo(other: HasId): Int = {
      (this.id, other.id) match {
        case (LAnonId(a), LAnonId(b)) => (b - a).toInt
        case (a, b) => a.toString.compareTo(b.toString)
      }
    }
  }

  final class LNamedId(override val name: String) extends LId {
    override def toString = name 
  }

  final case class LAnonId(id: Long) extends LId {
    require(id >= 0)
    
    override def name: String = "$" + id

    override def toString = name 
  }
  
  def newAnonId: LAnonId = LAnonId(ids.next())

  private val anonIdNameRegex = "anon\\$(\\d+)".r
  
  def fromName(name: String): LId = name match {
    case anonIdNameRegex(id) => LAnonId(id.toLong)
    case _ => new LNamedId(name)
  }
  
  private val ids: Sequence[Long] = Sequence()
  
  trait IdBasedEquality { self: HasId =>
    override def equals(other: Any): Boolean = other match {
      case that: HasId if this.getClass == that.getClass => this.id == that.id
      case _ => false
    }
    
    override def hashCode: Int = id.hashCode
  }
}
