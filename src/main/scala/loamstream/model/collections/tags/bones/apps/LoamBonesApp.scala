package loamstream.model.collections.tags.bones.apps

import loamstream.model.collections.tags.{LNil, LoamMethodTag, LoamHeapRangeTag, LoamKeyTag}

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
object LoamBonesApp extends App {

  val inputSet = LoamKeyTag.node[Int].node[String].node[Double].getLSet
  val inputMap = LoamKeyTag.node[String].node[Char].getLMap[String]
  val outputSet = LoamKeyTag.node[Int].node[String].node[Double].getLSet
  val outputMap = LoamKeyTag.node[Byte].node[String].getLMap[Int]

  val inputs = inputSet :: inputMap :: LNil
  val outputs = outputMap :: outputSet :: LNil

  val method = LoamMethodTag(inputs, outputs)

  println(inputSet)
  println(inputMap)
  println(outputSet)
  println(outputMap)
  println(method)


}
