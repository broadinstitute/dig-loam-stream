package loamstream.model.collections.tags

/**
 * LoamStream
 * Created by oliverr on 10/22/2015.
 */
case class LoamMethodTag[HI <: LoamHeapTag[_, _], RI <: LoamHeapRangeTag[_, _], HO <: LoamHeapTag[_, _],
RO <: LoamHeapRangeTag[_, _]](inputs: LoamHeapRangeTag[HI, RI], outputs: LoamHeapRangeTag[HO, RO]) {

  override def toString: String = "" + inputs + " => " + outputs

}
