package loamstream.loam.intake

import scala.util.Try
import scala.util.Success
import loamstream.util.Tries

/**
 * @author clint
 */
sealed abstract class TechType(val name: String) {
  def unapply(s: String): Boolean = s == name //TODO: case insensitive?
  
  override def toString: String = name
}

object TechType {
  case object Gwas extends TechType("GWAS")
  case object ExChip extends TechType("ExChip")
  case object ExSeq extends TechType("ExSeq")
  case object Fm extends TechType("FM")
  case object IChip extends TechType("IChip")
  case object Wgs extends TechType("WGS")
  
  val values: Set[TechType] = Set(
      Gwas,
      ExChip,
      ExSeq,
      Fm,
      IChip,
      Wgs)
      
  private lazy val namesToValues: Map[String, TechType] = values.iterator.map(tt => tt.name.toLowerCase -> tt).toMap
  
  def fromString(s: String): Option[TechType] = {
    val normalized = s.toLowerCase
    
    namesToValues.get(normalized)
  }
  
  def tryFromString(s: String): Try[TechType] = fromString(s) match {
    case Some(tt) => Success(tt)
    case _ => Tries.failure(s"Unknown tech type string: '${s}'")
  }
}