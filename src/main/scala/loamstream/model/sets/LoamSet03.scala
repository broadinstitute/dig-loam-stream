package loamstream.model.sets

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet03[K00, K01, K02] extends LoamSet {

  type E = (K00, K01, K02)

  override def up[K03]: LoamSet04[K00, K01, K02, K03]

}
