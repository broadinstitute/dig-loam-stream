package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection03[K00, K01, K02] extends LoamCollection {

  type K = (K00, K01, K02)

  def up[K03]: LoamCollection04[K00, K01, K02, K03]

}
