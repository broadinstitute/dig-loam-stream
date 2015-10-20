package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection09
import loamstream.model.collections.sets.LoamSet09

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap09[K00, K01, K02, K03, K04, K05, K06, K07, K08, V]
  extends LoamMap with LoamCollection09[K00, K01, K02, K03, K04, K05, K06, K07, K08] {

  def keys: LoamSet09[K00, K01, K02, K03, K04, K05, K06, K07, K08]

  def up[K09]: LoamMap10[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, V]

}
