package loamstream.model.piles

import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
case class LPile(spec: LPileSpec) {
  def sig: LSig = spec.sig

  def kind: LKind = spec.kind

  def as(newKind: LKind): LPile = copy(spec = spec.copy(kind = newKind))

  def =:=(oPile: LPile): Boolean = kind == oPile.kind && sig =:= oPile.sig

  def <:<(oPile: LPile): Boolean = kind <:< oPile.kind && sig =:= oPile.sig

  def >:>(oPile: LPile): Boolean = kind >:> oPile.kind && sig =:= oPile.sig
}
