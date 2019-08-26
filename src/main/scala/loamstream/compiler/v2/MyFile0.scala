package loamstream.compiler.v2

object MyFile0 extends LoamFile {
  import loam._
  
  //val foo: URI = URI.create("")
  
  val in = store("/la/la/la.txt")
  val out = store("foo-out.txt")
  
  val tool = cmd"foo --bar $in $out".in(in).out(out)
  
  logFooter()
}
