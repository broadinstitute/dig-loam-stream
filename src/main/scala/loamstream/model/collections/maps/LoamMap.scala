package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection
import loamstream.model.collections.sets.LoamSet

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap extends LoamCollection {

  def keys: LoamSet

  def up[KN]: LoamMap

}
