package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection12
import loamstream.model.collections.sets.LoamSet12

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap12[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, V]
  extends LoamMap with LoamCollection12[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11] {

  def keys: LoamSet12[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11]

  def up[K12]: LoamMap13[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12, V]

}
