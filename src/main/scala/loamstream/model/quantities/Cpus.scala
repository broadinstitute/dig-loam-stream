package loamstream.model.quantities


/**
 * @author clint
 * Mar 7, 2017
 */
final case class Cpus(value: Int = 1) {
  require(value > 0)
  
  def isSingle: Boolean = value == 1
}
