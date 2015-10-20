package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection10
import loamstream.model.collections.sets.LoamSet10

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap10[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, V]
  extends LoamMap with LoamCollection10[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09] {

  def keys: LoamSet10[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09]

  def up[K10]: LoamMap11[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, V]

}
