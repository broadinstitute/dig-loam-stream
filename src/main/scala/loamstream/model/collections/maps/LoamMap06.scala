package loamstream.model.collections.maps

import loamstream.model.collections.LoamCollection06
import loamstream.model.collections.sets.LoamSet06

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamMap06[K00, K01, K02, K03, K04, K05, V]
  extends LoamMap with LoamCollection06[K00, K01, K02, K03, K04, K05] {

  def keys: LoamSet06[K00, K01, K02, K03, K04, K05]

  def up[K06]: LoamMap07[K00, K01, K02, K03, K04, K05, K06, V]

}
