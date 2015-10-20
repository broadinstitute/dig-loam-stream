package loamstream.model.sets

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet00 extends LoamSet {

  type E = Nil.type

  override def up[K01]: LoamSet01[K01]

}
