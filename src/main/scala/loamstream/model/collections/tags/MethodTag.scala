package loamstream.model.collections.tags

/**
 * LoamStream
 * Created by oliverr on 10/22/2015.
 */
case class MethodTag[IR <: LoamHeapRangeTag[_, _], OR <: LoamHeapRangeTag[_, _]](inputs: IR, outputs: OR) {

  override def toString: String = "" + inputs + " => " + outputs

}
