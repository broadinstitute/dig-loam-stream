package loamstream.model.collections.tags

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamHeapRangeTag {

  def apply[H <: LoamHeapTag[_,_]](heap: H): LoamHeapRangeTag[H, Nothing] =
    LoamHeapRangeTag[H, Nothing](heap, None)

}

object LNil {
  def ::[CN <: LoamHeapTag[_, _]](heapTag: CN): LoamHeapRangeTag[CN, Nothing] =
    LoamHeapRangeTag[CN, Nothing](heapTag, None)
}

case class LoamHeapRangeTag[H <: LoamHeapTag[_, _], P <: LoamHeapRangeTag[_, _]](heap: H, parentOpt: Option[P]) {

  def ::[CN <: LoamHeapTag[_, _]](heapTag: CN): LoamHeapRangeTag[CN, this.type] =
    LoamHeapRangeTag[CN, this.type](heapTag, Some(this))

  def toSeq: Seq[LoamHeapTag[_, _]] = heap +: parentOpt.map(_.toSeq).getOrElse(Nil)

  override def toString: String = toSeq.mkString("(", ",", ")")

}
