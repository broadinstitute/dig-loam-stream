package loamstream.model.collections.tags.bones.apps

import loamstream.model.collections.tags.maps.LoamMapTag02
import loamstream.model.collections.tags.sets.LoamSetTag03


/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
object LoamBonesApp extends App {

  val inputSet = LoamSetTag03.create[Int, String, Double]
  val inputMap = LoamMapTag02.create[String, Char, String]
  val outputSet = LoamSetTag03.create[Int, String, Double]
  val outputMap = LoamMapTag02.create[Byte, String, Int]

  val method = LoamMethodTag(inputSet, inputMap, outputSet, outputMap)

  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)
  println(method)


}
