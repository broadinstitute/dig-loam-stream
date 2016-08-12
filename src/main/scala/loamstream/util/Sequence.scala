package loamstream.util

/**
 * @author clint
 * date: Aug 12, 2016
 */
final class Sequence[N](start: N, step: N)(implicit evidence: Numeric[N]) {
  private val current: ValueBox[N] = ValueBox(start)
  
  def next(): N = {
    def inc(n: N): N = evidence.plus(n, step)
    
    current.getAndUpdate(i => (inc(i), i))
  }
}

object Sequence {
  def apply[N: Numeric](start: N = 0, step: N = 1): Sequence[N] = new Sequence(start, step)
}