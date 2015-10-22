package loamstream.model.collections.tags

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
object LoamHeapRangeTag {

  def apply[C <: LoamHeapTag[_]](collection: C): LoamHeapRangeTag[C, Nothing] =
    LoamHeapRangeTag[C, Nothing](collection, None)

}

case class LoamHeapRangeTag[H <: LoamHeapTag[_], P <: LoamHeapRangeTag[_, _]](heap: H, parentOpt: Option[P]) {

  def :+[CN <: LoamHeapTag[_]](inputTag: CN): LoamHeapRangeTag[CN, this.type] =
    LoamHeapRangeTag[CN, this.type](inputTag, Some(this))

  def toSeq: Seq[LoamHeapTag[_]] = heap +: parentOpt.map(_.toSeq).getOrElse(Nil)

  override def toString: String = toSeq.mkString("(", ",", ")")

}
