package loamstream.loam.intake

import scala.util.Try
import scala.util.Success
import loamstream.util.Tries

/**
 * @author clint
 * 
 * 9 Mar, 2021
 */
sealed abstract class Ancestry(val name: String) {
  def unapply(s: String): Boolean = s == name //TODO: case insensitive?
  
  override def toString: String = name
}

object Ancestry {
  case object AA extends Ancestry("AA")
  case object AF extends Ancestry("AF")
  case object EA extends Ancestry("EA")
  case object EU extends Ancestry("EU")
  case object HS extends Ancestry("HS")
  case object Mixed extends Ancestry("Mixed")
  case object SA extends Ancestry("SA")
  
  val values: Set[Ancestry] = Set(
      AA,
      AF,
      EA,
      EU,
      HS,
      Mixed,
      SA)
      
  private lazy val namesToValues: Map[String, Ancestry] = values.iterator.map(tt => tt.name.toLowerCase -> tt).toMap
  
  def fromString(s: String): Option[Ancestry] = {
    val normalized = s.toLowerCase
    
    namesToValues.get(normalized)
  }
  
  def tryFromString(s: String): Try[Ancestry] = fromString(s) match {
    case Some(tt) => Success(tt)
    case _ => Tries.failure(s"Unknown tech type string: '${s}'")
  }
}