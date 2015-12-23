package loamstream.model.calls

import loamstream.model.tags.LPileTag

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LPileCall[Tag <: LPileTag[_, _]] {
  def tag: Tag
}
