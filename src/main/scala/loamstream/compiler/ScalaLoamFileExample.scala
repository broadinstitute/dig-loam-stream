package loamstream.compiler

import loamstream.loam.ScalaLoamScript
import loamstream.util.Files
import java.nio.file.Paths
import com.typesafe.config.ConfigFactory
import loamstream.conf.LoamConfig
import loamstream.util.code.PackageId

object ScalaLoamFileExample extends App {
  val compiler = LoamCompiler.default
  
  val scriptA = ScalaLoamScript(
      name = "A", 
      pkg = PackageId("loamstream", "compiler"),
      code = Files.readFrom(Paths.get("src/main/scala/loamstream/compiler/A.scala")))
      
  val scriptB = ScalaLoamScript(
      name = "B",
      pkg = PackageId("loamstream", "compiler"),
      code = Files.readFrom(Paths.get("src/main/scala/loamstream/compiler/B.scala")))
  
  val config = LoamConfig.fromString("{}").get
  
  val project = LoamProject(config, scriptA, scriptB)
  
  val result = compiler.compile(project)
  
  println(result)
}
