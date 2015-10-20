package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection00
import loamstream.model.collections.sets.LoamSet00

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap00[V] extends LoamMap with LoamCollection00 {

  def keys: LoamSet00

  def up[K00]: LoamMap01[K00, V]

}
