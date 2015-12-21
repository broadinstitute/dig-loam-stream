package loamstream.model.streams.apps

import loamstream.model.streams.atoms.maps.LMapAtom02
import loamstream.model.streams.atoms.sets.LSetAtom03
import loamstream.model.tags.maps.LMapTag02
import loamstream.model.tags.methods.LMethodTag2I2O
import loamstream.model.tags.sets.LSetTag03

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
object LStreamApp1 extends App {

  val inputSet = LSetAtom03.create[Int, String, Double]("InputSet")
  val inputMap = LMapAtom02.create[String, Char, String]("InputMap")
  val outputSet = LSetAtom03.create[Int, String, Double]("OutputSet")
  val outputMap = LMapAtom02.create[Byte, String, Int]("OutputMap")


  //  val edgeBroken0 = LEdge(inputSet, method.input1)  //  example of illegal use
  //  val edgeBroken1 = LEdge(outputMap, method.output0)  //  example of illegal use

  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)

}
