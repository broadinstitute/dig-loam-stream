package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection10[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09] extends LoamCollection {

  type K = (K00, K01, K02, K03, K04, K05, K06, K07, K08, K09)

  def up[K10]: LoamCollection11[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10]

}
