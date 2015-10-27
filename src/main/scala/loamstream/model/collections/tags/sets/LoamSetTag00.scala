package loamstream.model.collections.tags.sets

import loamstream.model.collections.tags.heaps.LoamHeapTag00
import loamstream.model.collections.tags.keys.LoamKeyTag00

import scala.reflect.runtime.universe.{TypeTag, typeTag}

/**
 * LoamStream
 * Created by oliverr on 10/26/2015.
 */
case object LoamSetTag00 extends LoamSetTag with LoamHeapTag00 {
  override def key = LoamKeyTag00

  override def plusKey[K00: TypeTag] = LoamSetTag01[K00](typeTag[K00])
}
