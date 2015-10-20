package loamstream.model.collections.sets

import loamstream.model.collections.LoamCollection

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet extends LoamCollection {

  def up[KN]: LoamSet

}
