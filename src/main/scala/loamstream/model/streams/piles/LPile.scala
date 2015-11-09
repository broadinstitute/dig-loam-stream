package loamstream.model.streams.piles

import loamstream.model.streams.piles.LPile.Namer
import loamstream.model.streams.pots.piles.LPilePot
import loamstream.model.tags.maps.LMapTag
import loamstream.model.tags.piles.LPileTag
import loamstream.model.tags.sets.LSetTag

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

trait LPile {
  type PTag <: LPileTag

  def tag: PTag

  def addKey[KN]: LPilePot[_] = addKey[KN](Namer.Default)

  def addKey[KN](namer: Namer): LPilePot[_]
}
