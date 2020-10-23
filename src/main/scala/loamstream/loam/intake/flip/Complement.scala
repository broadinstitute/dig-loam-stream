package loamstream.loam.intake.flip

/**
 * @author clint
 * Apr 1, 2020
 * 
 * Ported from Marcin's Perl code
 */
object Complement extends (String => String) {
  override def apply(s: String): String = s match {
    case "A" => "T"
    case "C" => "G"
    case "T" => "A"
    case "G" => "C"
  }
}
