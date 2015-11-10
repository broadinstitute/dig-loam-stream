package loamstream.model.streams.piles

import loamstream.model.streams.LNode
import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.pots.piles.LPilePot
import loamstream.model.tags.maps.LMapTag
import loamstream.model.tags.piles.LPileTag
import loamstream.model.tags.sets.LSetTag

import scala.language.higherKinds
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
object LPile {

  object Namer {

    object Default extends Namer {
      var i = 0

      override def name(tag: LPileTag): String = {
        val baseName = tag match {
          case set: LSetTag => "set"
          case map: LMapTag[_] => "map"
          case _ => "pile"
        }
        baseName + i
      }
    }

  }

  trait Namer {
    def name(tag: LPileTag): String
  }

}

trait LPile extends LNode {
  type Tag = PTag
  type PTag <: LPileTag
  type Parent[_] <: LPilePot[LPile]

  def tag: PTag

  def plusKey[KN: TypeTag]: Parent[KN] = plusKey[KN](Namer.Default)

  def plusKey[KN: TypeTag](namer: Namer): Parent[KN]
}
