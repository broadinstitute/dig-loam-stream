package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection05
import loamstream.model.collections.sets.LoamSet05

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap05[K00, K01, K02, K03, K04, V] extends LoamMap with LoamCollection05[K00, K01, K02, K03, K04] {

  def keys: LoamSet05[K00, K01, K02, K03, K04]

  def up[K05]: LoamMap06[K00, K01, K02, K03, K04, K05, V]

}
