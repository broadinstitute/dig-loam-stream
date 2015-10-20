package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection06[K00, K01, K02, K03, K04, K05] extends LoamCollection {

  type K = (K00, K01, K02, K03, K04, K05)

  def up[K06]: LoamCollection07[K00, K01, K02, K03, K04, K05, K06]

}
