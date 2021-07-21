package loamstream.loam.intake

import scala.util.Try
import scala.util.Success
import loamstream.util.Tries

/**
 * @author clint
 * 17 Mar, 2021
 */
sealed abstract class UploadType(val name: String, val s3Dir: String)

object UploadType {
  case object Variants extends UploadType("variants", "variants")
  case object VariantCounts extends UploadType("variant_counts", "variant_counts")
  //TODO: FIXME: totally arbitrary s3 dir
  case object GeneLevel extends UploadType("gene_level", "genes")
  
  val values: Set[UploadType] = Set(Variants, VariantCounts)
  
  private lazy val byName: Map[String, UploadType] = values.map(v => (v.name.toLowerCase, v)).toMap
  
  def fromString(s: String): Option[UploadType] = byName.get(s.toLowerCase)
  
  def tryFromString(s: String): Try[UploadType] = fromString(s) match {
    case Some(ut) => Success(ut)
    case None => Tries.failure(s"Unknown upload type '$s'")
  }
}