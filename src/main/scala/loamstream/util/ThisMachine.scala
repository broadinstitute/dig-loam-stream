package loamstream.util

/**
 * @author clint
 * Aug 24, 2020
 */
object ThisMachine {
  def numCpus: Int = Runtime.getRuntime.availableProcessors
}
