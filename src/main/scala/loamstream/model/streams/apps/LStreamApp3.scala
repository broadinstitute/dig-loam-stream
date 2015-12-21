package loamstream.model.streams.apps

import loamstream.model.streams.atoms.maps.LMapAtom02
import loamstream.model.streams.atoms.sets.LSetAtom02
import loamstream.model.streams.piles.LPile.Namer

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
object LStreamApp3 extends App {

  val inputSetChild = LSetAtom02.create[Int, String]("InputSetChild")
  val inputSet = inputSetChild.plusKey[Double](Namer.Default)
  val inputMap = LMapAtom02.create[String, Char, String]("InputMap")
  val outputSetChild = LSetAtom02.create[Int, String]("OutputSetChild")
  val outputSet = outputSetChild.plusKey[Double](Namer.Default)
  val outputMap = LMapAtom02.create[Byte, String, Int]("OutputMap")

  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)

}
