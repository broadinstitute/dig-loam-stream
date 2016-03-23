package loamstream.util

import utils.Loggable

import scala.reflect.runtime.universe.typeTag

/**
  * LoamStream
  * Created by oliverr on 1/20/2016.
  */
object ProductTypeExploderApp extends App with Loggable {

  class A[T]

  object A {

    class B

  }

  debug(ProductTypeExploder.explode(typeTag[(Int, String, A[Int], A.B, Double)].tpe).toString)
  debug(ProductTypeExploder.explode(typeTag[String].tpe).toString)
}
