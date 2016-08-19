package loamstream.util

/**
 * @author clint
 * date: Aug 8, 2016
 * 
 * An Iterator that takes some action - invokes the passed-in onFinished block - when it is exhausted, and 
 * delegates to another Iterator for everything else.
 */
final class TakesEndingActionIterator[A](delegate: Iterator[A])(onFinished: => Any) extends Iterator[A] {
  private[this] val invokedOnFinish: ValueBox[Boolean] = ValueBox(false)
  
  override def hasNext: Boolean = delegate.hasNext

  override def next(): A = {
    try { delegate.next() }
    finally {
      invokedOnFinish.foreach { invoked => 
        if(!hasNext && !invoked) {
          invokedOnFinish() = true
        
          onFinished
        }
      }
    }
  }
}

object TakesEndingActionIterator {
  def apply[A](delegate: Iterator[A])(onFinished: => Any): TakesEndingActionIterator[A] = {
    new TakesEndingActionIterator(delegate)(onFinished)
  }
}