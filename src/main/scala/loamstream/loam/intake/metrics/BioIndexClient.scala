package loamstream.loam.intake.metrics

/**
 * @author clint
 * Mar 18, 2020
 */
trait BioIndexClient {
  def isKnown(varId: String): Boolean
  
  final def isUnknown(varId: String): Boolean = !isKnown(varId)
}
