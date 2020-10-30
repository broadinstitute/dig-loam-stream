package loamstream.loam.intake

import loamstream.loam.intake.flip.Complement

/**
 * @author clint
 * Apr 1, 2020
 */
final case class Variant(chrom: String, pos: Int, ref: String, alt: String) {
   
  require(ref.nonEmpty)
  require(alt.nonEmpty)
  
  def underscoreDelimited: String = delimitedBy('_')

  def colonDelimited: String = delimitedBy(':')
  
  def complement: Variant = copy(ref = Complement(ref), alt = Complement(alt))
  
  def complementIf(shouldBeComplemented: Boolean): Variant = if(shouldBeComplemented) complement else this
  
  def flip: Variant = copy(ref = alt, alt = ref)
  
  def flipIf(shouldBeFlipped: Boolean): Variant = if(shouldBeFlipped) flip else this
  
  def flipped: String = flip.colonDelimited
  
  def delimitedBy(delimiter: Char): String = s"${chrom}${delimiter}${pos}${delimiter}${ref}${delimiter}${alt}"
  
  def asBioIndexCoord: String = s"chr${chrom}:${pos}"
  
  def asFullBioIndexCoord: String = s"chr${colonDelimited}"
  
  def toUpperCase: Variant = copy(chrom = chrom.toUpperCase, ref = ref.toUpperCase, alt = alt.toUpperCase)
  
  def isSingleNucleotide: Boolean = ref.size == 1 && alt.size == 1
  
  def isMultiNucleotide: Boolean = ref.size > 1 || alt.size > 1
}
  
object Variant {
  def unapply(s: String): Option[Variant] = s match {
    case Regexes.underscoreDelimited(chrom, pos, ref, alt) => Some(Variant(chrom, pos.toInt, ref, alt))
    case Regexes.spaceDelimited(chrom, pos, ref, alt) => Some(Variant(chrom, pos.toInt, ref, alt))
    case Regexes.colonDelimited(chrom, pos, ref, alt) => Some(Variant(chrom, pos.toInt, ref, alt))
    case _ => None
  }
  
  def from(varId: String): Variant = {
    unapply(varId).getOrElse(sys.error(s"Couldn't determine chromosome and position from variant ID '${varId}'"))
  }
  
  object Regexes {
    val underscoreDelimited= """^(.+?)_(\d+?)_(.+?)_(.+?)$""".r
    
    val spaceDelimited= """^(.+?)\s(\d+?)\s(.+?)\s(.+?)$""".r
    
    val colonDelimited= """^(.+?)\:(\d+?)\:(.+?)\:(.+?)$""".r
  }
}
