package loamstream.model.collections.tags

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
case class LoamSetTag[K <: LoamKeyTag[_, _]](keys: K) extends LoamHeapTag[K] {
  override def toString: String = "LSet(" + keys + ")"
}
