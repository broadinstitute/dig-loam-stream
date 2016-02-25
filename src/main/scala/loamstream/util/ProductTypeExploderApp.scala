package loamstream.util

import loamstream.util.shot.{Hit, Miss}

import scala.reflect.runtime.universe.typeTag

/**
  * LoamStream
  * Created by oliverr on 1/20/2016.
  */
object ProductTypeExploderApp extends App {

  class A[T]

  object A {

    class B

  }

  ProductTypeExploder.explode(typeTag[(Int, String, A[Int], A.B, Double)].tpe) match {
    case Hit(tagList) => println(tagList)
    case Miss(snag) => println(snag)
  }
}
