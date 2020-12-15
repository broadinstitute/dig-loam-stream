package loamstream.loam.intake.flip

import loamstream.loam.intake.Variant
import scala.util.matching.Regex

/**
 * @author clint
 * Apr 1, 2020
 */
final case class RichVariant(
      referenceFiles: ReferenceFiles,
      variantsFrom26k: java.util.Set[String],
      variant: Variant) {
    
  def flip: RichVariant = copy(variant = variant.flip)
  
  def chrom: String = variant.chrom 
  def position: Int = variant.pos
  def alt: String = variant.alt 
  def reference: String = variant.ref
  
  def toKey: String = variant.underscoreDelimited
  
  def toKeyComplemented: String = variant.complement.underscoreDelimited
  
  def isIn26k: Boolean = variantsFrom26k.contains(this.toKey)
  
  def isIn26kComplemented: Boolean = variantsFrom26k.contains(this.toKeyComplemented)
  
  def refCharFromReferenceGenome: Option[Char] = referenceFiles.getChar(chrom, position - 1) 
  
  def refFromReferenceGenome: Option[String] = referenceFiles.getString(chrom, position - 1, reference.size)
  
  def altFromReferenceGenome: Option[String] = referenceFiles.getString(chrom, position - 1, alt.size)
}
