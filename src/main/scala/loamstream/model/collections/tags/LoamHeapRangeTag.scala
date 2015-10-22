package loamstream.model.collections.tags

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamHeapRangeTag {

  def apply[C <: LoamHeapTag[_]](collection: C): LoamHeapRangeTag[C, Nothing] =
    new LoamHeapRangeTag[C, Nothing](collection, None)

}

class LoamHeapRangeTag[C <: LoamHeapTag[_], P <: LoamHeapRangeTag[_, _]]
(val collection: C, val parentOpt: Option[P]) {

  def :+[CN <: LoamHeapTag[_]](inputTag: CN): LoamHeapRangeTag[CN, this.type] =
    new LoamHeapRangeTag[CN, this.type](inputTag, Some(this))

}
