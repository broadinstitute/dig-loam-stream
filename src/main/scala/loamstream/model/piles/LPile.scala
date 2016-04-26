package loamstream.model.piles

import loamstream.model.id.LId
import loamstream.model.id.LId.LNamedId
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object LPile {
  def apply(name: String, sig: LSig, kind: LKind): LPile = LPile(LNamedId(name), LPileSpec(sig, kind))

  def apply(sig: LSig, kind: LKind): LPile = LPile(LId.newAnonId, LPileSpec(sig, kind))
}

case class LPile(id: LId, spec: LPileSpec) extends LId.Owner {
  override val ownerBaseName = "pile"
}