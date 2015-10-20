package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection04
import loamstream.model.collections.sets.LoamSet04

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap04[K00, K01, K02, K03, V] extends LoamMap with LoamCollection04[K00, K01, K02, K03] {

  def keys: LoamSet04[K00, K01, K02, K03]

  def up[K04]: LoamMap05[K00, K01, K02, K03, K04, V]

}
