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
  implicit def javaCloseablesCanBeClosed[C <: Closeable]: CanBeClosed[C] = _.close()

  implicit def scalaSourcesCanBeClosed[S <: Source]: CanBeClosed[S] = _.close()
  
  implicit def terminablesCanBeClosed[A <: Terminable]: CanBeClosed[A] = _.stop()

  def using[A, C: CanBeClosed](c: C)(f: C => A): A = enclosed(c)(f)
  
  def enclosed[A, C: CanBeClosed](c: C)(f: C => A): A = {
    try {
      f(c)
    } finally {
      val closer = implicitly[CanBeClosed[C]]

      closer.close(c)
    }
  }
  
  implicit final class Syntax[C: CanBeClosed](c: C) {
    def close(): Unit = implicitly[CanBeClosed[C]].close(c)
  }
}
