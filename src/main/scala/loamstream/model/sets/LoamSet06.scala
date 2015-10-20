package loamstream.model.sets

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet06[K00, K01, K02, K03, K04, K05] extends LoamSet {

  type E = (K00, K01, K02, K03, K04, K05)

  override def up[K06]: LoamSet07[K00, K01, K02, K03, K04, K05, K06]

}
