package loamstream.model

import scala.reflect.runtime.universe.Type

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
final case class StoreSpec(sig: Type) {
  def =:=(other: StoreSpec): Boolean = sig =:= other.sig

  def <:<(other: StoreSpec): Boolean = sig =:= other.sig

  def >:>(other: StoreSpec): Boolean = sig =:= other.sig
}
