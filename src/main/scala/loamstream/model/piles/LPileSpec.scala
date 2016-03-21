package loamstream.model.piles

import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
case class LPileSpec(sig: LSig, kind: LKind) {
  def as(newKind: LKind): LPileSpec = copy(kind = newKind)

  def =:=(oPile: LPileSpec): Boolean = kind == oPile.kind && sig =:= oPile.sig

  def <:<(oPile: LPileSpec): Boolean = {
    val pileKindAndSigMatch = kind <:< oPile.kind && sig =:= oPile.sig
    val pileKindMatch = kind <:< oPile.kind
    val pileSigMatch = sig =:= oPile.sig
    pileKindAndSigMatch
  }

  def >:>(oPile: LPileSpec): Boolean = kind >:> oPile.kind && sig =:= oPile.sig
}
