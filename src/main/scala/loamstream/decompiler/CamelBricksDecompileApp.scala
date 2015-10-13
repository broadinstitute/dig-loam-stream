package loamstream.decompiler

import loamstream.CamelBricksFiles
import loamstream.camelbricks.CamelBricksPipeline

/**
 * LoamStream
 * Created by oliverr on 10/13/2015.
 */
object CamelBricksDecompileApp extends App {

  def decompileMta: CamelBricksPipeline = CamelBricksDecompiler.decompile(CamelBricksFiles.mtaAsSource)

  def decompileTargeted: CamelBricksPipeline = CamelBricksDecompiler.decompile(CamelBricksFiles.targetedAsSource)

  val pipeline = decompileTargeted
  
  for(entry <- pipeline.entries) {
    println(entry.asString)
  }

}
