package loamstream.model

import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
case class StoreSpec(sig: LSig, kind: LKind) {
  def as(newKind: LKind): StoreSpec = copy(kind = newKind)

  def =:=(other: StoreSpec): Boolean = kind == other.kind && sig =:= other.sig

  def <:<(other: StoreSpec): Boolean = kind <:< other.kind && sig =:= other.sig

  def >:>(other: StoreSpec): Boolean = kind >:> other.kind && sig =:= other.sig
}
