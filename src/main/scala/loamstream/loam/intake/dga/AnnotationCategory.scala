package loamstream.loam.intake.dga

import loamstream.loam.intake.ColumnTransforms
import scala.util.Try
import scala.util.Success
import loamstream.util.Tries

/**
 * @author clint
 * 30 Apr, 2021
 */
sealed abstract class AnnotationCategory(val name: String) {
  require(
      AnnotationCategory.noWhitespaceIn(name), 
      s"Annotation category '$name' not allowed: can't contain whitespace")
  
  def unapply(s: String): Boolean = s == name
}

object AnnotationCategory {
  case object CisRegulatoryElements extends AnnotationCategory("cis-regulatory_elements")
  case object GeneExpressionLevels extends AnnotationCategory("gene_expression_levels")
  case object GeneticVariantEffects extends AnnotationCategory("genetic_variant_effects")
  case object Others extends AnnotationCategory("others")
  case object TargetGeneLinks extends AnnotationCategory("target_gene_links")
  case object VariantGeneLinks extends AnnotationCategory("variant_gene_links")

  lazy val values: Set[AnnotationCategory] = Set(
      CisRegulatoryElements,
      GeneExpressionLevels,
      GeneticVariantEffects,
      Others,
      TargetGeneLinks,
      VariantGeneLinks)
      
  private lazy val namesToValues: Map[String, AnnotationCategory] = values.map(at => at.name.toLowerCase -> at).toMap
  
  private def normalize(s: String): String = ColumnTransforms.doNormalizeSpaces(requireNonEmpty = false)(s).toLowerCase
  
  def unsafeFromString(s: String): AnnotationCategory = tryFromString(s).get
  
  def fromString(s: String): Option[AnnotationCategory] = namesToValues.get(normalize(s))
  
  def tryFromString(s: String): Try[AnnotationCategory] = fromString(s) match {
    case Some(ac) => Success(ac)
    case _ => Tries.failure(s"Unknown annotation type string: '${s}'")
  }
            
  private def noWhitespaceIn(s: String): Boolean = s.forall(!_.isWhitespace)
}