package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection02[K00, K01] extends LoamCollection {

  type K = (K00, K01)

  def up[K02]: LoamCollection03[K00, K01, K02]

}
