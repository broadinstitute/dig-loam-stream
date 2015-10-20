package loamstream.model.collections.sets

import loamstream.model.collections.{LoamCollection03, LoamCollection02}

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet03[K00, K01, K02] extends LoamSet with LoamCollection03[K00, K01, K02] {

  override def up[K03]: LoamSet04[K00, K01, K02, K03]

}
