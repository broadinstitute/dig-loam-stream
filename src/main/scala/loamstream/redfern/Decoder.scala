package loamstream.redfern

import loamstream.util.shot.Shot
import org.openrdf.model.Resource

/**
  * LoamStream
  * Created by oliverr on 4/12/2016.
  */
trait Decoder[T] {

  def read(redFern: RedFern, root: Resource): Shot[T]

}
