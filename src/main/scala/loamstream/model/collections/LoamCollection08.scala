package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection08[K00, K01, K02, K03, K04, K05, K06, K07] extends LoamCollection {

  type K = (K00, K01, K02, K03, K04, K05, K06, K07)

  def up[K08]: LoamCollection09[K00, K01, K02, K03, K04, K05, K06, K07, K08]

}
