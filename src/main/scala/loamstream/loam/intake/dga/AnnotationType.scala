package loamstream.loam.intake.dga

import scala.util.Success
import scala.util.Try

import loamstream.util.Tries
import loamstream.loam.intake.LiteralColumnExpr
import loamstream.loam.intake.ColumnTransforms

/**
 * @author clint
 * Feb 23, 2021
 */
sealed abstract class AnnotationType(val name: String) {
  require(AnnotationType.noWhitespaceIn(name), s"Annotation name '$name' not allowed: can't contain whitespace")
  
  def unapply(s: String): Boolean = s == name
}

object AnnotationType {
  
  case object AccessibleChromatin extends AnnotationType("accessible_chromatin")
  case object BindingSites extends AnnotationType("binding_sites")
  case object CandidateRegulatoryElements extends AnnotationType("candidate_regulatory_elements")
  case object CaQTL extends AnnotationType("caqtl")
  case object ChromatinState extends AnnotationType("chromatin_state")
  case object EQTL extends AnnotationType("eqtl")
  case object GeneExpression extends AnnotationType("gene_expression")
  case object HistoneModifications extends AnnotationType("histone_modifications")
  case object HQTL extends AnnotationType("hQTL")
  case object MeQTL extends AnnotationType("meQTL")
  case object Other extends AnnotationType("other")
  case object RepresentativeDNaseHypersensitivitySites extends 
      AnnotationType("representative_DNase_hypersensitivity_sites")
  case object SQTL extends AnnotationType("sQTL")
  case object TargetGenePredictions extends AnnotationType("target_gene_predictions")
  case object VariantAllelicEffects extends AnnotationType("variant_allelic_effects")
  case object VariantToGene extends AnnotationType("variant_to_gene")
  
  lazy val values: Set[AnnotationType] = Set(
      AccessibleChromatin,
      BindingSites,
      CandidateRegulatoryElements,
      CaQTL,
      ChromatinState,
      EQTL,
      GeneExpression,
      HistoneModifications,
      HQTL,
      MeQTL,
      Other,
      RepresentativeDNaseHypersensitivitySites, 
      SQTL,
      TargetGenePredictions,
      VariantAllelicEffects,
      VariantToGene)
      
  private lazy val namesToValues: Map[String, AnnotationType] = values.map(at => at.name.toLowerCase -> at).toMap
  
  private def normalize(s: String): String = ColumnTransforms.doNormalizeSpaces(requireNonEmpty = false)(s).toLowerCase
  
  def unsafeFromString(s: String): AnnotationType = tryFromString(s).get
  
  def fromString(s: String): Option[AnnotationType] = namesToValues.get(normalize(s))
  
  def tryFromString(s: String): Try[AnnotationType] = fromString(s) match {
    case Some(at) => Success(at)
    case _ => Tries.failure(s"Unknown annotation type string: '${s}'")
  }
  
  private def noWhitespaceIn(s: String): Boolean = s.forall(!_.isWhitespace)
}
