package loamstream.model.sets

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet02[K00, K01] extends LoamSet {

  type E = (K00, K01)

  override def up[K02]: LoamSet03[K00, K01, K02]

}
