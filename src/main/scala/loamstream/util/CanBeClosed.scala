package loamstream.util

import scala.io.Source
import java.io.Closeable

/**
 * @author clint
 * date: Mar 11, 2016
 */
trait CanBeClosed[C] {
  def close(c: C): Unit
}

object CanBeClosed {
  implicit def javaCloseablesCanBeClosed[C <: Closeable]: CanBeClosed[C] = (c: C) => c.close()

  implicit def scalaSourcesCanBeClosed[S <: Source]: CanBeClosed[S] = (s: S) => s.close()

  def enclosed[A, C: CanBeClosed](c: C)(f: C => A): A = {
    try {
      f(c)
    } finally {
      val closer = implicitly[CanBeClosed[C]]

      closer.close(c)
    }
  }
}
