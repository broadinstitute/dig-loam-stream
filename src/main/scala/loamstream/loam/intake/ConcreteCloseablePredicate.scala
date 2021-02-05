package loamstream.loam.intake

import java.io.Closeable

/**
 * @author clint
 * Oct 14, 2020
 */
final class ConcreteCloseablePredicate[A](toClose: Closeable)(p: Predicate[A]) extends Predicate[A] with Closeable {
  override def apply(a: A): Boolean = p(a)

  override def close(): Unit = toClose.close()
}

object ConcreteCloseablePredicate {
  def apply[A](toClose: Closeable)(p: Predicate[A]): ConcreteCloseablePredicate[A] = {
    new ConcreteCloseablePredicate(toClose)(p)
  }
}
