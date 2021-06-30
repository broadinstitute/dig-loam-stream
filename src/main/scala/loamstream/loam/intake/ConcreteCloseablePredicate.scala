package loamstream.loam.intake

import java.io.Closeable
import loamstream.util.Throwables
import loamstream.util.Loggable

/**
 * @author clint
 * Oct 14, 2020
 */
final class ConcreteCloseablePredicate[A](toClose: Iterable[Closeable])(p: Predicate[A]) extends Predicate[A] with Closeable {
  override def apply(a: A): Boolean = p(a)

  override def close(): Unit = {
    implicit object FailsafeLogger extends Loggable

    toClose.foreach(c => Throwables.quietly(s"Couldn't close $c")(c.close()))
  }
}

object ConcreteCloseablePredicate {
  def apply[A](toClose: Closeable, rest: Closeable*)(p: Predicate[A]): ConcreteCloseablePredicate[A] = {
    new ConcreteCloseablePredicate(toClose +: rest.toSeq)(p)
  }
}
