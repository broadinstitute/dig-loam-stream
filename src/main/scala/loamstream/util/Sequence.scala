package loamstream.util

/**
 * @author clint
 * date: Aug 12, 2016
 * 
 * A class representing an unbounded sequence of numbers, starting from 'start' and advancing by 'step'.
 * 'start' and 'step' can be positive, negative, or a combination.
 * 
 * Use cases are primarily anonymous IDs. 
 * 
 * @param start the first element in this Sequence
 * @param step the amount to advance the Sequence by
 * @param N the type of values produced by the sequence.  An instance of scala.Numeric[N] needs to be available.
 * By default, this means we can make Sequences of Bytes, Shorts, Ints, Longs, Doubles, Floats, and possibly more. 
 */
final class Sequence[N] private (val start: N, val step: N)(implicit evidence: Numeric[N]) extends Iterable[N] {
  private val current: ValueBox[N] = ValueBox(start)
  
  override def toString: String = s"${getClass.getSimpleName}(start = $start, step = $step, current = ${current()})"
  
  /**
   * @return the next value in this Sequence
   */
  def next(): N = {
    def inc(n: N): N = evidence.plus(n, step)
    
    current.getAndUpdate(i => (inc(i), i))
  }
  
  override def iterator: Iterator[N] = Iterator.continually(next())
}

object Sequence {
  /**
   * Make a Sequence of Ns
   * 
   * @param start the first element in this Sequence - default is 0
   * @param step the amount to advance the Sequence by - default is 1
   * @param N the type of values produced by the sequence.  An instance of scala.Numeric[N] needs to be available.
   */
  def apply[N]()(implicit ev: Numeric[N]): Sequence[N] = apply(ev.zero, ev.one)
  
  def apply[N: Numeric](start: N, step: N): Sequence[N] = new Sequence(start, step)
}