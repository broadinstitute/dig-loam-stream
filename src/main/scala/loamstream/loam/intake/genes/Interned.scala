package loamstream.loam.intake.genes

import loamstream.util.ValueBox

object Interned {
  import scala.language.higherKinds

  abstract class Companion[A, B, M[_, _]: InternStorage](makeB: A => B) {
    private def ev = implicitly[InternStorage[M]]

    private[this] val interned: ValueBox[M[A, B]] = ValueBox(ev.initial)

    def apply(a: A): B = interned.getAndUpdate { soFar =>
      //TODO: validate?

      ev.get(soFar, a) match { 
        case Some(b) => (soFar, b)
        case None => {
          val b = makeB(a)

          val newM = ev.add(soFar, a, b)

          (newM, b)
        }
      }
    }
  }

  trait InternStorage[M[_, _]] {
    def initial[A, B]: M[A, B]

    def add[A, B](m: M[A, B], a: A, b: B): M[A, B]

    def get[A, B](m: M[A, B], a: A): Option[B]
  }

  object InternStorage {

    implicit object ScalaImmutableMapInternStorage extends InternStorage[Map] {
      override def initial[A, B]: Map[A, B] = Map.empty

      override def add[A, B](m: Map[A, B], a: A, b: B): Map[A, B] = m + (a -> b)

      override def get[A, B](m: Map[A, B], a: A): Option[B] = m.get(a)
    }

    import java.{util => ju}

    implicit object JavaUtilMapInternStorage extends InternStorage[ju.Map] {
      override def initial[A, B]: ju.Map[A, B] = new ju.HashMap

      override def add[A, B](m: ju.Map[A, B], a: A, b: B): ju.Map[A, B] = {
        m.put(a, b)

        m
      }

      override def get[A, B](m: ju.Map[A, B], a: A): Option[B] = Option(m.get(a))
    }
  }
}
