package loamstream.compiler

import loamstream.loam.asscala.LoamFile

object A extends LoamFile {
  def inc(i: Int): Int = i + 1
  
  val t = cmd"echo ${inc(B.b)}"
  
  println(t)
}
