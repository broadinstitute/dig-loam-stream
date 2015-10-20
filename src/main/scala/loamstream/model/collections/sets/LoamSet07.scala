package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection07

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet07[K00, K01, K02, K03, K04, K05, K06]
  extends LoamSet with LoamCollection07[K00, K01, K02, K03, K04, K05, K06] {

  override def up[K07]: LoamSet08[K00, K01, K02, K03, K04, K05, K06, K07]

}
