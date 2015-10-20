package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection04[K00, K01, K02, K03] extends LoamCollection {

  type K = (K00, K01, K02, K03)

  def up[K04]: LoamCollection05[K00, K01, K02, K03, K04]

}
