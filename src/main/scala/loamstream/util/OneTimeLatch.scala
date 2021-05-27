package loamstream.util

/**
 * @author clint
 * Mar 31, 2017
 */
final class OneTimeLatch {
  private[this] val hasRunFlag: ValueBox[Boolean] = ValueBox(false)
  
  def doOnce(block: => Any): Boolean = {
    hasRunFlag.getAndUpdate { hasRun =>
      if(!hasRun) {
        block
      }
      
      (true, hasRun)
    }
  }
}
