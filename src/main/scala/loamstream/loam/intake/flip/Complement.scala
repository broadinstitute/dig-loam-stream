package loamstream.loam.intake.flip

/**
 * @author clint
 * Apr 1, 2020
 * 
 * Ported from Marcin's Perl code
 */
object Complement extends (String => String) {
  override def apply(s: String): String = s.length match {
    case 0 => sys.error("Can't complement an empty string")
    case 1 => singleNucleotide(s)
    case _ => multiNucleotide(s)
  }
  
  private def singleNucleotide(s: String): String = s match {
    case "A" => "T"
    case "C" => "G"
    case "T" => "A"
    case "G" => "C"
  }
  
  private def multiNucleotide(s: String): String = {
    s.replaceAll("A", "X")
     .replaceAll("T", "A")
     .replaceAll("X", "T")
     .replaceAll("C", "X")
     .replaceAll("G", "C")
     .replaceAll("X", "G")
  }
}
