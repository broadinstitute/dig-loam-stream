package loamstream.model.collections.tags

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LoamMapTag[K <: LoamKeyTag[_, _], V] extends LoamHeapTag[K] {

  def keySet: LoamSetTag[K]

}
