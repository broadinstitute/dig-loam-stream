package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection14

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet14[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12, K13]
  extends LoamSet with LoamCollection14[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12, K13] {

  override def up[K14]: LoamSet

}
