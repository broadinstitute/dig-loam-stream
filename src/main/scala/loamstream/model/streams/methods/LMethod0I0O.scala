package loamstream.model.streams.methods

import loamstream.model.streams.methods.LMethod.Namer
import loamstream.model.streams.pots.methods.LMethodPot0I0O
import loamstream.model.tags.methods.LMethodTag0I0O

import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/29/2015.
  */
trait LMethod0I0O extends LMethod {
  type MTag = LMethodTag0I0O.type
  type Parent[KN] = LMethodPot0I0O
  val tag = LMethodTag0I0O

  def plusKey[KN: TypeTag](namer: Namer) = LMethodPot0I0O(namer.name(tag.plusKey[KN]), this)
}
