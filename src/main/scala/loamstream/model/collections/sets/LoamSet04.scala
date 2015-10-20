package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection04

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet04[K00, K01, K02, K03] extends LoamSet with LoamCollection04[K00, K01, K02, K03] {

  override def up[K04]: LoamSet05[K00, K01, K02, K03, K04]

}
