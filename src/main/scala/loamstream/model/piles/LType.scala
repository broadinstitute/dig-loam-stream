package loamstream.model.piles

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

import scala.reflect.runtime.universe.Type


class LType(val tpe: Type) {

  override def equals(o: Any): Boolean = {
    o match {
      case o: LType => tpe =:= o.tpe
      case _ => false
    }
  }

  override def hashCode: Int = {
    tpe.toString.hashCode
  }

}
