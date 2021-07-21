package loamstream.loam.intake.genes

/**
  * @author clint
  * @date Jul 19, 20212
  *
  * @param value
  */
final class EnsemblId private (val value: String) extends AnyVal

object EnsemblId extends Interned.Companion[String, EnsemblId, Map](new EnsemblId(_)) {
  
  override def apply(s: String): EnsemblId = {
    require(isEnsembleId(s))

    super.apply(s)
  }

  private[genes] def isEnsembleId(s: String): Boolean = {
    //TODO: More?
    //TODO: TEST
    s.startsWith("ENSG") 
  }
}