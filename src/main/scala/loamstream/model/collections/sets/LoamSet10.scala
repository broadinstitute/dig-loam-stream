package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection10

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet10[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09]
  extends LoamSet with LoamCollection10[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09] {

  override def up[K10]: LoamSet11[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10]

}
