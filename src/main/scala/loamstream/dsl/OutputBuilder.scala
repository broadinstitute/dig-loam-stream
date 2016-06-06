package loamstream.dsl

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 5/26/2016.
  */
object OutputBuilder {
  def apply[T: TypeTag](name: String): OutputBuilder = OutputBuilder(name, typeTag[T].tpe)
}

case class OutputBuilder(name: String, tpe: Type) {

}
