package loamstream.compiler.v2

object MyFile1 extends LoamFile {
  import loam._
  
  //val foo: URI = URI.create("")
  
  val in = MyFile0.out
  val out = store("/x/y/z")
  
  val tool = cmd"bar --baz $in $out".in(in).out(out)
  
  logFooter()
}

