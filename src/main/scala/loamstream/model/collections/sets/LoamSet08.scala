package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection08

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet08[K00, K01, K02, K03, K04, K05, K06, K07]
  extends LoamSet with LoamCollection08[K00, K01, K02, K03, K04, K05, K06, K07] {

  override def up[K08]: LoamSet09[K00, K01, K02, K03, K04, K05, K06, K07, K08]

}
