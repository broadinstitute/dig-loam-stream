package loamstream.model.values

/**
  * LoamStream
  * Created by oliverr on 4/19/2016.
  */
case class LValue(value: Any, tpe: LType) {
  def as(tpe2: LType): LValue = LValue(value, tpe2)

  def valueAs[T]: T = value.asInstanceOf[T]
}

object LValue {

  object Implicits {

    final implicit class LTupleOps[A](val lhs: A)(implicit evA: HasLType[A]) {
      def &[B](rhs: B)(implicit evB: HasLType[B]): LValue = {
        (evA.lType & evB.lType).of(lhs -> rhs)
      }
    }

  }

}