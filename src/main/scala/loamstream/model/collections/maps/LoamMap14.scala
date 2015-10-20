package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection14
import loamstream.model.collections.sets.LoamSet14

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap14[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12, K13, V]
  extends LoamMap with LoamCollection14[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12, K13] {

  def keys: LoamSet14[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12, K13]

  def up[K14]: LoamMap

}
