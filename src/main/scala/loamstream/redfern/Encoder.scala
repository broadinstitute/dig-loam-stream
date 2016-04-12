package loamstream.redfern

import org.openrdf.model.Resource

/**
  * LoamStream
  * Created by oliverr on 4/12/2016.
  */
trait Encoder[T] {

  def encode(redFern: RedFern, root: Resource, thing: T): Unit

}
