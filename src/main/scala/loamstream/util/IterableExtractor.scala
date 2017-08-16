package loamstream.util

import scala.collection.IterableLike
import scala.language.higherKinds

/**
  * LoamStream
  * Created by oruebenacker on 11.08.17.
  */
class IterableExtractor[E, I[EE] <: IterableLike[EE, I[EE]]] {
  def unapply(any: Any): Option[I[E]] = any match {
    case iterable: I[E] if iterable.forall(_.isInstanceOf[E]) => Some(iterable)
    case _ => None
  }
}

object IterableExtractor {
  def newFor[E, I[EE] <: IterableLike[EE, I[EE]]]: IterableExtractor[E, I] = new IterableExtractor[E, I]
}
