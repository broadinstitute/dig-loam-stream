package loamstream.model.sets

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamSet {

  type E

  def up[K]: LoamSet

}
