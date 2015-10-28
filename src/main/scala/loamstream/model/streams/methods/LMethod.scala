package loamstream.model.streams.methods

import loamstream.model.tags.methods.LMethodTag

/**
 * LoamStream
 * Created by oliverr on 10/28/2015.
 */
trait LMethod[T <: LMethodTag] {
  def tag: T
}
