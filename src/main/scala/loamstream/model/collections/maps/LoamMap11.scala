package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection11
import loamstream.model.collections.sets.LoamSet11

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap11[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, V]
  extends LoamMap with LoamCollection11[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10] {

  def keys: LoamSet11[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10]

  def up[K11]: LoamMap12[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, V]

}
