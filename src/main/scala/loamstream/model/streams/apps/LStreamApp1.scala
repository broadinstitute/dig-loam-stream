package loamstream.model.streams.apps

import loamstream.model.streams.atoms.maps.LMapAtom02
import loamstream.model.streams.atoms.methods.LMethodAtom2I2O
import loamstream.model.streams.atoms.sets.LSetAtom03
import loamstream.model.streams.edges.LEdge
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

  val method =
    LMethodAtom2I2O("TheMethod", LMethodTag2I2O(LSetTag03.create[Int, String, Double],
      LMapTag02.create[String, Char, String], LSetTag03.create[Int, String, Double],
      LMapTag02.create[Byte, String, Int]))

  val edgeI0 = LEdge(inputSet, method.input0)
  val edgeI1 = LEdge(inputMap, method.input1)
  val edgeO0 = LEdge(outputSet, method.output0)
  val edgeO1 = LEdge(outputMap, method.output1)

  //  val edgeBroken0 = LEdge(inputSet, method.input1)  //  example of illegal use
  //  val edgeBroken1 = LEdge(outputMap, method.output0)  //  example of illegal use


  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)
  println(method)


}
