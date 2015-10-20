package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection02
import loamstream.model.collections.sets.LoamSet02

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap02[K00, K01, V] extends LoamMap with LoamCollection02[K00, K01] {

  def keys: LoamSet02[K00, K01]

  def up[K02]: LoamMap03[K00, K01, K02, V]

}
