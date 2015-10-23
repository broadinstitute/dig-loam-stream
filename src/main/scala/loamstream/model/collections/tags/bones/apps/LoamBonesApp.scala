package loamstream.model.collections.tags.bones.apps

import loamstream.model.collections.tags.{LKey, LNil, LoamMethodTag}

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
object LoamBonesApp extends App {

  val inputSet = LKey.key[Int].key[String].key[Double].getLSet
  val inputMap = LKey.key[String].key[Char].getLMap[String]
  val outputSet = LKey.key[Int].key[String].key[Double].getLSet
  val outputMap = LKey.key[Byte].key[String].getLMap[Int]

  val inputs = inputSet :: inputMap :: LNil
  val outputs = outputMap :: outputSet :: LNil

  val method = LoamMethodTag(inputs, outputs)

  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)
  println(method)


}
