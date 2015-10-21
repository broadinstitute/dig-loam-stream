package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LoamMapTag[K <: LoamKeyTag[_, _], V] extends LoamCollectionTag[K] {

  def keySet: LoamSetTag[K]

}
