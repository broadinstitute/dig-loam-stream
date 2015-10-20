package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection07
import loamstream.model.collections.sets.{LoamSet07, LoamSet06}

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap07[K00, K01, K02, K03, K04, K05, K06, V]
  extends LoamMap with LoamCollection07[K00, K01, K02, K03, K04, K05, K06] {

  def keys: LoamSet07[K00, K01, K02, K03, K04, K05, K06]

  def up[K07]: LoamMap08[K00, K01, K02, K03, K04, K05, K06, K07, V]

}
