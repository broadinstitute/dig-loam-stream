package loamstream.util

/**
 * @author clint
 * date: Jun 1, 2016
 */
object IteratorEnrichments {
  final implicit class IteratorOps[A](val iter: Iterator[A]) extends AnyVal {
    //TODO: Inefficient, quick-and-dirty
    def takeUntil(pred: A => Boolean): Iterator[A] = {
      val negated: A => Boolean = a => !pred(a)
      
      val stream = iter.toStream

      val s = stream.takeWhile(negated) ++ (stream.dropWhile(negated).headOption.filter(pred))
      
      s.iterator
    }
  }
}