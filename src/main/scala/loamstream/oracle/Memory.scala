package loamstream.oracle


/**
 * @author clint
 * Mar 7, 2017
 */
final case class Memory(bytes: Long) {
  def gb: Long = bytes / Memory.bytesPerGB
  def mb: Long = bytes / Memory.bytesPerMB
  def kb: Long = bytes / Memory.bytesPerKB
  
  def *(factor: Long): Memory = if(factor == 1) this else Memory(bytes * factor)
  def /(factor: Long): Memory = if(factor == 1) this else Memory(bytes / factor)
  
  def double: Memory = this * 2
}

object Memory {
  private val bytesPerKB: Long = 1024L
  private val bytesPerMB: Long = 1024L * bytesPerKB
  private val bytesPerGB: Long = 1024L * bytesPerMB
  
  def inGb(howMany: Int): Memory = Memory(howMany * bytesPerGB)
  def inGb(howMany: Double): Memory = Memory((howMany * bytesPerGB.toDouble).toLong)
  def inMb(howMany: Int): Memory = Memory(howMany * bytesPerMB)
  def inKb(howMany: Int): Memory = Memory(howMany * bytesPerKB)
}
