package loamstream.loam.intake

/**
 * @author clint
 * Apr 1, 2020
 */
final case class Variant(chrom: String, pos: Int, alt: String, ref: String) {
  def underscoreDelimited: String = delimitedBy('_')

  def colonDelimited: String = delimitedBy(':')
  
  def flip: Variant = copy(ref = alt, alt = ref)
  
  def delimitedBy(delimiter: Char): String = s"${chrom}${delimiter}${pos}${delimiter}${alt}${delimiter}${ref}"
  
  def asBioIndexCoord: String = s"chr${chrom}:${pos}"
}
  
object Variant {
  def unapply(s: String): Option[Variant] = s match {
    case Regexes.underscoreDelimited(chrom, pos, alt, ref) => Some(Variant(chrom, pos.toInt, alt, ref))
    case Regexes.spaceDelimited(chrom, pos, alt, ref) => Some(Variant(chrom, pos.toInt, alt, ref))
    case Regexes.colonDelimited(chrom, pos, alt, ref) => Some(Variant(chrom, pos.toInt, alt, ref))
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
