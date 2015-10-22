package loamstream.model.collections.tags.bones.apps

import loamstream.model.collections.tags.LoamKeyTag

/**
 * LoamStream
 * Created by oliverr on 10/20/2015.
 */
object LoamBonesApp extends App {

  val keyTag = LoamKeyTag.node[Int].node[String].node[Double]

  println(keyTag)

}
