package loamstream.model.values

/**
 * LoamStream
 * Created by oliverr on 4/19/2016.
 */
case class LValue[T](value: T, tpe: LType[T])

object LValue {
  object Implicits {
    import HasLType._

    final implicit class LTupleOps[A](val lhs: A)(implicit evA: HasLType[A]) {
      def &[B](rhs: B)(implicit evB: HasLType[B]): LValue[(A, B)] = {
        (evA.lType & evB.lType).of(lhs -> rhs)
      }
    }
  }
}