package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection03
import loamstream.model.collections.sets.LoamSet03

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap03[K00, K01, K02, V] extends LoamMap with LoamCollection03[K00, K01, K02] {

  def keys: LoamSet03[K00, K01, K02]

  def up[K03]: LoamMap04[K00, K01, K02, K03, V]

}
