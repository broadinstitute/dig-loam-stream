package loamstream.model.sets

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet05[K00, K01, K02, K03, K04] extends LoamSet {

  type E = (K00, K01, K02, K03, K04)

  override def up[K05]: LoamSet06[K00, K01, K02, K03, K04, K05]

}
