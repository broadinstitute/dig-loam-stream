package loamstream.model

import scala.util.Random

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

  case class LAnonId(time: Long, random: Long) extends LId {
    override def name: String = time + "_" + random
  }
  
  private val random = new Random

  private[model] def positiveRandomLong: Long = {
    Iterator.continually(random.nextLong()).dropWhile(_ <= 0).next()
  }

  def newAnonId: LAnonId = LAnonId(System.currentTimeMillis, positiveRandomLong)

  private val anonIdNameRegex = "\\(d+)_\\(d+)".r
  
  def fromName(name: String): LId = name match {
    case anonIdNameRegex(time, rand) => LAnonId(time.toLong, rand.toLong)
    case _ => LNamedId(name)
  }
}
