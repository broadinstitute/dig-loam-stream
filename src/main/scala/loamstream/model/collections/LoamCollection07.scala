package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection07[K00, K01, K02, K03, K04, K05, K06] extends LoamCollection {

  type K = (K00, K01, K02, K03, K04, K05, K06)

  def up[K07]: LoamCollection08[K00, K01, K02, K03, K04, K05, K06, K07]

}
