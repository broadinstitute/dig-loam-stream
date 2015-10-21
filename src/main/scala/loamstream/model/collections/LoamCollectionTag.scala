package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LoamCollectionTag[K <: LoamKeyTag[_, _]] {

  def keys: K

}
