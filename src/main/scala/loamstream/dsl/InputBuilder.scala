package loamstream.dsl

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 5/26/2016.
  */
object InputBuilder {
  def apply[T: TypeTag](name: String): InputBuilder = InputBuilder(name, typeTag[T].tpe)
}

case class InputBuilder(name: String, tpe: Type) {

}
