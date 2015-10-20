package loamstream.model.sets

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet01[K00] extends LoamSet {

  type E = K00

  override def up[K01]: LoamSet02[K00, K01]

}
