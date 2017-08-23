package loamstream.util

import scala.language.higherKinds
import scala.reflect.{ClassTag, classTag}

/**
  * LoamStream
  * Created by oruebenacker on 11.08.17.
  */
class TypedIterableExtractor[E: ClassTag] {
  def unapply(any: Any): Option[Iterable[E]] = {
    val eClass = classTag[E].runtimeClass
    any match {
      case iterable: Iterable[_] if iterable.forall(element => eClass.isAssignableFrom(element.getClass)) =>
        Some(iterable.asInstanceOf[Iterable[E]])
      case _ =>
        None
    }
  }
}

object TypedIterableExtractor {
  def newFor[E: ClassTag]: TypedIterableExtractor[E] = new TypedIterableExtractor[E]
}
