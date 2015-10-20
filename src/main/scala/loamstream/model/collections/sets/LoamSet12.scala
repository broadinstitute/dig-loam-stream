package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection12

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet12[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11]
  extends LoamSet with LoamCollection12[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11] {

  override def up[K12]: LoamSet13[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11, K12]

}
