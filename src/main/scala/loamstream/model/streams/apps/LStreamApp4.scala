package loamstream.model.streams.apps

import loamstream.model.streams.atoms.maps.LMapAtom02
import loamstream.model.streams.atoms.methods.LMethodAtom2I2O
import loamstream.model.streams.atoms.sets.LSetAtom02
import loamstream.model.streams.edges.LEdge
import loamstream.model.streams.methods.LMethod.{Namer => MethodNamer}
import loamstream.model.streams.piles.LPile.{Namer => PileNamer}
import loamstream.model.tags.maps.LMapTag01
import loamstream.model.tags.methods.LMethodTag2I2O
import loamstream.model.tags.sets.LSetTag02

/**
  * LoamStream
  * Created by oliverr on 10/28/2015.
  */
object LStreamApp4 extends App {

  val inputSetChild = LSetAtom02.create[Int, String]("InputSetChild")
  val inputSet = inputSetChild.plusKey[String](PileNamer.Default)
  val inputMap = LMapAtom02.create[String, Char, String]("InputMap")
  val outputSetChild = LSetAtom02.create[Int, String]("OutputSetChild")
  val outputSet = outputSetChild.plusKey[String](PileNamer.Default)
  val outputMap = LMapAtom02.create[Byte, String, String]("OutputMap")

  val methodChild =
    LMethodAtom2I2O("TheMethod", LMethodTag2I2O(LSetTag02.create[Int, String],
      LMapTag01.create[String, Char], LSetTag02.create[Int, String],
      LMapTag01.create[Byte, String]))

  val method2 = methodChild.plusKey[String](MethodNamer.Default)

//  val edgeI0 = LEdge(inputSet, method.input0)
//  val edgeI1 = LEdge(inputMap, method.input1)
//  val edgeO0 = LEdge(outputSet, method.output0)
//  val edgeO1 = LEdge(outputMap, method.output1)

  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)
  println(methodChild)
  println(method2)


}
