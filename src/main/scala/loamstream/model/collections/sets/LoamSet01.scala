package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection01

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet01[K00] extends LoamSet with LoamCollection01[K00] {

  override def up[K01]: LoamSet02[K00, K01]

}
