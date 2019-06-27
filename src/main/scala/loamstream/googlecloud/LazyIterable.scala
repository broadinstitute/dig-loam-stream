package loamstream.googlecloud

import com.google.cloud.storage.Blob

/**
 * @author clint
 * Jun 27, 2019
 */
final class LazyIterable[A] private (getIterator: => Iterator[A]) extends Iterable[A] {
  override def iterator: Iterator[A] = getIterator
}

object LazyIterable {
  def apply[A](getIterator: => Iterator[A]): LazyIterable[A] = new LazyIterable(getIterator)
}
