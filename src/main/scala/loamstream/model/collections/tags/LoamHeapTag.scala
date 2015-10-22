package loamstream.model.collections.tags

/**
 * LoamStream
 * Created by oliverr on 10/21/2015.
 */
trait LoamHeapTag[K <: LoamKeyTag[_, _]] {

  def keys: K

}
