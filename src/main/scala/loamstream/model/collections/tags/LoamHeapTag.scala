package loamstream.model.collections.tags

import scala.reflect.runtime.universe.TypeTag

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LoamHeapTag[T, P <: LoamKeyTag[_, _]] {

  def withKey[TC: TypeTag]: LoamHeapTag[TC, LoamKeyTag[T, P]]

}
