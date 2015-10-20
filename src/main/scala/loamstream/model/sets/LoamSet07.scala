package loamstream.model.sets

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet07[K00, K01, K02, K03, K04, K05, K06] extends LoamSet {

  type E = (K00, K01, K02, K03, K04, K05, K06)

  override def up[K07]: LoamSet08[K00, K01, K02, K03, K04, K05, K06, K07]

}
