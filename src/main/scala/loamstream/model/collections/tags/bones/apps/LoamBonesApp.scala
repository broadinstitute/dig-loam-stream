package loamstream.model.collections.tags.bones.apps

import loamstream.model.collections.tags.maps.LMapTag02
import loamstream.model.collections.tags.sets.LSetTag03


/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
object LoamBonesApp extends App {

  val inputSet = LSetTag03.create[Int, String, Double]
  val inputMap = LMapTag02.create[String, Char, String]
  val outputSet = LSetTag03.create[Int, String, Double]
  val outputMap = LMapTag02.create[Byte, String, Int]

  val method = LoamMethodTag(inputSet, inputMap, outputSet, outputMap)

  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)
  println(method)


}
