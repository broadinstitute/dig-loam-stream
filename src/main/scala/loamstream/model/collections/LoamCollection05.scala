package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection05[K00, K01, K02, K03, K04] extends LoamCollection {

  type K = (K00, K01, K02, K03, K04)

  def up[K05]: LoamCollection06[K00, K01, K02, K03, K04, K05]

}
