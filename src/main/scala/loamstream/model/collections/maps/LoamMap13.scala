package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection13
import loamstream.model.collections.sets.LoamSet13

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap13[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12, V]
  extends LoamMap with LoamCollection13[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12] {

  def keys: LoamSet13[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12]

  def up[K13]: LoamMap14[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12, K13, V]

}
