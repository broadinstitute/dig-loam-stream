package loamstream.loam.intake.flip

import loamstream.loam.intake.Variant
import scala.util.matching.Regex

/**
 * @author clint
 * Apr 1, 2020
 */
final class RichVariant(
      referenceFiles: ReferenceFiles,
      variantsFrom26k: Set[String],
      variant: Variant) {
    
  def chrom: String = variant.chrom 
  def position: Int = variant.pos
  def alt: String = variant.alt 
  def reference: String = variant.ref
  
  def toKey: String = s"${chrom}_${position}_${alt}_${reference}"
  
  def toKeyMunged: String = s"${chrom}_${position}_${N2C(alt)}_${N2C(reference)}"
  
  def isIn26k: Boolean = variantsFrom26k.contains(this.toKey)
  
  def isIn26kMunged: Boolean = variantsFrom26k.contains(this.toKeyMunged)
  
  def refChar: Option[Char] = referenceFiles.getChar(chrom, position - 1) 
  
  def refFromReferenceGenome: Option[String] = referenceFiles.getString(chrom, position - 1, reference.size)
  
  def altFromReferenceGenome: Option[String] = referenceFiles.getString(chrom, position - 1, alt.size)
}
  
object RichVariant {
  final case class Extractor(regex: Regex, referenceFiles: ReferenceFiles, variantsFrom26k: Set[String]) {
    def unapply(s: String): Option[RichVariant] = s match {
      case regex(c, p, a, r) => Some(new RichVariant(referenceFiles, variantsFrom26k, Variant(c, p.toInt, a, r)))
      case _ => None
    }
  }
}
