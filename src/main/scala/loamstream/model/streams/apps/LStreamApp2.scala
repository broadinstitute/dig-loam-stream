package loamstream.model.streams.apps

import loamstream.model.streams.atoms.maps.LMapAtom02
import loamstream.model.streams.atoms.sets.LSetAtom02
import loamstream.model.streams.pots.sets.LSetPot03

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
object LStreamApp2 extends App {

  val inputSetChild = LSetAtom02.create[Int, String]("InputSetChild")
  val inputSet = LSetPot03.create[Int, String, Double]("InputSet", inputSetChild)
  val inputMap = LMapAtom02.create[String, Char, String]("InputMap")
  val outputSetChild = LSetAtom02.create[Int, String]("OutputSetChild")
  val outputSet = LSetPot03.create[Int, String, Double]("OutputSet", outputSetChild)
  val outputMap = LMapAtom02.create[Byte, String, Int]("OutputMap")

  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)

}
