package loamstream.loam.intake

import java.io.Closeable

/**
 * @author clint
 * Oct 14, 2020
 */
final class ConcreteCloseableTransform[A](toClose: Closeable)(t: Transform[A]) extends Transform[A] with Closeable {
  override def apply(a: A): A = t(a)
  
  override def close(): Unit = toClose.close()
}

object ConcreteCloseableTransform {
  def apply[A](toClose: Closeable)(p: Transform[A]): ConcreteCloseableTransform[A] = {
    new ConcreteCloseableTransform(toClose)(p)
  }
}
