package loamstream.loam.intake.dga

import scala.util.Try
import scala.util.Success
import loamstream.util.Tries

/**
 * @author clint
 * Feb 23, 2021
 */
sealed abstract class AnnotationType(val name: String) {
  def unapply(s: String): Boolean = s == name
}

object AnnotationType {
  case object AccessibleChromatin extends AnnotationType("accessible_chromatin")
  case object ChromatinState extends AnnotationType("chromatin_state")
  case object BindingSites extends AnnotationType("binding_sites")
  case object TargetGenePrediction extends AnnotationType("target_gene_prediction")
  
  case object CandidateRegulatoryRegions extends AnnotationType("candidate_regulatory_regions")
  case object HistoneModifications extends AnnotationType("histone_modifications")
  case object VariantAllelicEffects extends AnnotationType("variant_allelic_effects")
  case object EQTL extends AnnotationType("eqtl")
  case object CaQTL extends AnnotationType("caqtl")
  case object BindingFootprint extends AnnotationType("binding_footprint")
  
  val values: Set[AnnotationType] = Set(
      AccessibleChromatin, 
      ChromatinState, 
      BindingSites, 
      TargetGenePrediction,
      CandidateRegulatoryRegions,
      HistoneModifications,
      VariantAllelicEffects,
      EQTL,
      CaQTL,
      BindingFootprint)
  
  def fromString(s: String): Option[AnnotationType] = s.trim.replaceAll("\\s+", "_").toLowerCase match {
    case AccessibleChromatin() => Some(AccessibleChromatin)
    case ChromatinState() => Some(ChromatinState)
    case BindingSites() => Some(BindingSites)
    case TargetGenePrediction() => Some(TargetGenePrediction)
    
    case CandidateRegulatoryRegions() => Some(CandidateRegulatoryRegions)
    case HistoneModifications() => Some(HistoneModifications)
    case VariantAllelicEffects() => Some(VariantAllelicEffects)
    case EQTL() => Some(EQTL)
    case CaQTL() => Some(CaQTL)
    case BindingFootprint() => Some(BindingFootprint)
    case _ => None
  }
  
  def tryFromString(s: String): Try[AnnotationType] = fromString(s) match {
    case Some(at) => Success(at)
    case _ => Tries.failure(s"Unknown annotation type string: '${s}'")
  }
}
