package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection08
import loamstream.model.collections.sets.LoamSet08

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap08[K00, K01, K02, K03, K04, K05, K06, K07, V]
  extends LoamMap with LoamCollection08[K00, K01, K02, K03, K04, K05, K06, K07] {

  def keys: LoamSet08[K00, K01, K02, K03, K04, K05, K06, K07]

  def up[K08]: LoamMap09[K00, K01, K02, K03, K04, K05, K06, K07, K08, V]

}
