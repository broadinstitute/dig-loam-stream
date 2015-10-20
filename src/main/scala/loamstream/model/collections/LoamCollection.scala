package loamstream.model.collections

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
trait LoamCollection {

  type K

  def up[KN]: LoamCollection

}
