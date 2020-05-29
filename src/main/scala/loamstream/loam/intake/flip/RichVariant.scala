package loamstream.loam.intake.flip

import loamstream.loam.intake.Variant
import scala.util.matching.Regex

/**
 * @author clint
 * Apr 1, 2020
 */
final case class RichVariant(
      referenceFiles: ReferenceFiles,
      variantsFrom26k: Set[String],
      variant: Variant) {
    
  def flip: RichVariant = copy(variant = variant.flip)
  
  def chrom: String = variant.chrom 
  def position: Int = variant.pos
  def alt: String = variant.alt 
  def reference: String = variant.ref
  
  def toKey: String = s"${chrom}_${position}_${reference}_${alt}"
  
  def toKeyMunged: String = s"${chrom}_${position}_${N2C(reference)}_${N2C(alt)}"
  
  def isIn26k: Boolean = variantsFrom26k.contains(this.toKey)
  
  def isIn26kMunged: Boolean = variantsFrom26k.contains(this.toKeyMunged)
  
  def refChar: Option[Char] = referenceFiles.getChar(chrom, position - 1) 
  
  def refFromReferenceGenome: Option[String] = referenceFiles.getString(chrom, position - 1, reference.size)
  
  def altFromReferenceGenome: Option[String] = referenceFiles.getString(chrom, position - 1, alt.size)
}
  
object RichVariant {
  final case class Extractor(regex: Regex, referenceFiles: ReferenceFiles, variantsFrom26k: Set[String]) {
    def unapply(s: String): Option[RichVariant] = s match {
      case regex(c, p, r, a) => {
        Some(new RichVariant(referenceFiles, variantsFrom26k, Variant(chrom = c, pos = p.toInt, ref = r, alt = a)))
      }
      case _ => None
    }
  }
}
