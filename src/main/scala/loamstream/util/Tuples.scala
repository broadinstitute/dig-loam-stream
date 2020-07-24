package loamstream.util

/**
 * @author clint
 * Jul 24, 2020
 */
object Tuples {
  object Implicits {
    final implicit class Tuple2Ops[A, B](val t: (A, B)) extends AnyVal {
      def mapFirst[C](f: A => C): (C, B) = (f(t._1), t._2)
    
      def mapSecond[C](f: B => C): (A, C) = (t._1, f(t._2))
    }
  }
}
