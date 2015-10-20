package loamstream.model.sets

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet11[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10] extends LoamSet {

  type E = (K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10)

  override def up[K11]: LoamSet12[K00, K01, K02, K03, K04, K05, K06, K07, K08, K09, K10, K11]

}
