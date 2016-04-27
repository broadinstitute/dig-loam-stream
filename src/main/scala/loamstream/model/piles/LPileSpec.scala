package loamstream.model.piles

import loamstream.model.kinds.LKind
import loamstream.model.LSig

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
case class LPileSpec(sig: LSig, kind: LKind) {
  def as(newKind: LKind): LPileSpec = copy(kind = newKind)

  def =:=(oPile: LPileSpec): Boolean = kind == oPile.kind && sig =:= oPile.sig

  def <:<(oPile: LPileSpec): Boolean = kind <:< oPile.kind && sig =:= oPile.sig

  def >:>(oPile: LPileSpec): Boolean = kind >:> oPile.kind && sig =:= oPile.sig
}
