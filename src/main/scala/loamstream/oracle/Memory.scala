package loamstream.oracle

import squants.information._

/**
 * @author clint
 * Mar 7, 2017
 * 
 * A class representing some quantity of RAM.
 * 
 * TODO: Use Gibibytes, Mebibytes, etc?
 */
final case class Memory private (value: Information) {
  require(value.toBytes >= 0)
  
  def gb: Double = value.toGigabytes
  def mb: Double = value.toMegabytes
  def kb: Double = value.toKilobytes 
  
  def *(factor: Double): Memory = if(factor == 1) this else Memory(value * factor)
  
  def /(factor: Double): Memory = {
    require(factor != 0.0)
    
    if(factor == 1) this else Memory(value / factor)
  }
  
  def double: Memory = this * 2
}

object Memory {
  import squants.information.InformationConversions._
  
  def inBytes(howMany: Long): Memory = Memory(byte * howMany)
  
  def inGb(howMany: Double): Memory = Memory(gigabyte * howMany)
}
