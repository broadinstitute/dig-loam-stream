package loamstream.loam.intake

/**
  * @author clint
  * @date Jul 21, 2021
  */
object Alleles {
  def isAllowedAllele(ch: Char): Boolean = ch match {
    case 'A' | 'G' | 'C' | 'T' | 'a' | 'g' | 'c' | 't' => true
    case _ => false
  }

  def areAllowedAlleles(s: String): Boolean = {
    s.nonEmpty && {
      val noCommas = s.filter(_ != ',')
      
      noCommas.nonEmpty && noCommas.forall(isAllowedAllele)
    }
  }
}
