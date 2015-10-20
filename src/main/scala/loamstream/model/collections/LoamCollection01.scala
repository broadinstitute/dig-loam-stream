package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection01[K00] extends LoamCollection {

  type K = K00

  def up[K01]: LoamCollection02[K00, K01]

}
