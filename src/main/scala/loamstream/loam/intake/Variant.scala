package loamstream.loam.intake

import loamstream.loam.intake.flip.Complement
import scala.util.Try
import loamstream.util.Tries
import scala.util.Success
import scala.util.Failure

/**
 * @author clint
 * Apr 1, 2020
 */
final class Variant private (val chrom: String, val pos: Int, val ref: String, val alt: String) {
   
  require(ref.nonEmpty)
  require(alt.nonEmpty)
  
  override def toString: String = s"${getClass.getSimpleName}($chrom,$pos,$ref,$alt)"

  private def copy(
    chrom: String = this.chrom,
    pos: Int = this.pos, 
    ref: String = this.ref, 
    alt: String = this.alt): Variant = new Variant(chrom = chrom, pos = pos, ref = ref, alt = alt)

  override def hashCode: Int = Seq(chrom, pos, ref, alt).hashCode

  override def equals(other: Any): Boolean = other match {
    case that: Variant => {
      this.chrom == that.chrom && 
      this.pos == that.pos && 
      this.ref == that.ref && 
      this.alt == that.alt
    }
    case _ => false
  }

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
  
  def isMultiAllelic: Boolean = alt.exists(_ == ',') || ref.exists(_ == ',')
}

object Variant {
  private def makeVariant(raw: Option[String])(chrom: String, pos: String, ref: String, alt: String): Try[Variant] = {
    def normalize(str: String) = str.trim.toUpperCase.replaceAll("^\\,*", "").replaceAll("\\,*$", "")

    val normalizedRef = normalize(ref)
    val normalizedAlt = normalize(alt)

    val posAttempt = Try(pos.toInt)

    def orig = raw.getOrElse(s"<original-unknown> (chrom='$chrom', pos='$pos', ref='$ref', alt='$alt')")

    import Tries.failure

    if(!Alleles.areAllowedAlleles(normalizedRef)) { failure(s"Disallowed ref allele encountered in '$orig'") }
    else if(!Alleles.areAllowedAlleles(normalizedAlt)) { failure(s"Disallowed alt allele encountered in '$orig'") }
    else if(posAttempt.isFailure) { failure(s"Non-numeric position field encountered in '$raw'") }
    else {
      //TODO: More validations?  Known chrom?
      for {
        p <- posAttempt
      } yield {
        new Variant(chrom.trim.toUpperCase, pos = p, ref = normalizedRef, alt = normalizedAlt)
      }
    }
  }

  private def parse(varId: String): Try[(String, String, String, String)] = varId match {
    case Regexes.underscoreDelimited(chrom, pos, ref, alt) => Success((chrom, pos, ref, alt))
    case Regexes.spaceDelimited(chrom, pos, ref, alt) => Success((chrom, pos, ref, alt))
    case Regexes.colonDelimited(chrom, pos, ref, alt) => Success((chrom, pos, ref, alt))
    case _ => {
      Tries.failure(s"Couldn't extract chrom, pos, ref, and alt from '$varId'; tried '_', ' ', and ':' delimiters")
    }
  }

  private def parseAndMake(s: String): Try[Variant] = {
    parse(s).flatMap { case (chrom, pos, ref, alt) => makeVariant(Option(s))(chrom, pos, ref, alt) }
  }

  def unapply(s: String): Option[Variant] = parseAndMake(s).toOption
  
  def apply(varId: String): Variant = parseAndMake(varId).get

  def from(varId: String): Variant = apply(varId)

  def from(chrom: String, pos: String, ref: String, alt: String): Variant = {
    makeVariant(None)(chrom, pos, ref, alt).get
  }
  
  object Regexes {
    val underscoreDelimited= """^(.+?)_(\d+?)_(.+?)_(.+?)$""".r
    
    val spaceDelimited= """^(.+?)\s(\d+?)\s(.+?)\s(.+?)$""".r
    
    val colonDelimited= """^(.+?)\:(\d+?)\:(.+?)\:(.+?)$""".r
  }
}
