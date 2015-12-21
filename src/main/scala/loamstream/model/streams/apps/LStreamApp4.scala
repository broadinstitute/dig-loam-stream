package loamstream.model.streams.apps

import loamstream.model.streams.atoms.maps.LMapAtom02
import loamstream.model.streams.atoms.sets.LSetAtom02
import loamstream.model.streams.piles.LPile.{Namer => PileNamer}

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
object LStreamApp4 extends App {

  val inputSetChild = LSetAtom02.create[Int, String]("InputSetChild")
  val inputSet = inputSetChild.plusKey[String](PileNamer.Default)
  val inputMap = LMapAtom02.create[Char, String, Long]("InputMap")
  val outputSetChild = LSetAtom02.create[Int, String]("OutputSetChild")
  val outputSet = outputSetChild.plusKey[String](PileNamer.Default)
  val outputMap = LMapAtom02.create[Byte, String, String]("OutputMap")

  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)
}
