package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection00 extends LoamCollection {

  type K = Nil.type

  def up[K00]: LoamCollection01[K00]

}
