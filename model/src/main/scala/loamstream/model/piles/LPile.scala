package loamstream.model.piles

import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
case class LPile(sig: LSig, kind: LKind) {
  def as(newKind: LKind): LPile = copy(kind = newKind)

  def =:=(oPile: LPile): Boolean = kind == oPile.kind && sig =:= oPile.sig

  def <:<(oPile: LPile): Boolean = kind <:< oPile.kind && sig =:= oPile.sig

  def >:>(oPile: LPile): Boolean = kind >:> oPile.kind && sig =:= oPile.sig
}
