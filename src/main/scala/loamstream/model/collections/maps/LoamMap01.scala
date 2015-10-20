package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection01
import loamstream.model.collections.sets.LoamSet01

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap01[K00, V] extends LoamMap with LoamCollection01[K00] {

  def keys: LoamSet01[K00]

  def up[K01]: LoamMap02[K00, K01, V]

}
