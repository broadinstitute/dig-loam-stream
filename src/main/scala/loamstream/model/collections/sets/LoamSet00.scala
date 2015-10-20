package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection00

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet00 extends LoamSet with LoamCollection00 {

  override def up[K00]: LoamSet01[K00]

}
