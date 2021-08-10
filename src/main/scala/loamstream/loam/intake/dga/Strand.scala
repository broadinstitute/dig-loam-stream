package loamstream.loam.intake.dga

import scala.util.Try
import scala.util.Success
import loamstream.util.Tries

/**
 * @author clint
 * 20 Apr, 2021
 */
sealed abstract class Strand(val name: String) {
  override def toString: String = name
}

object Strand {
  case object Plus extends Strand("+")
  val `+`: Plus.type = Plus
  
  case object Minus extends Strand("-")
  val `-`: Minus.type = Minus
  
  lazy val values: Set[Strand] = byName.values.toSet
  
  private lazy val byName: Map[String, Strand] = Iterator(Plus, Minus).map(s => s.name -> s).toMap
  
  def fromString(s: String): Option[Strand] = byName.get(s.trim)
  
  def tryFromString(s: String): Try[Strand] = fromString(s) match {
    case Some(strand) => Success(strand)
    case _ => Tries.failure(s"Unknown strand string '${s}'")
  }
}