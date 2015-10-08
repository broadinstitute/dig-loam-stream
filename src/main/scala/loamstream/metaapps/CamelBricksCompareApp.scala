package loamstream.metaapps

import loamstream.CamelBricksFiles
import loamstream.compiler.CamelBricksCompiler
import loamstream.pipeline.Pipeline
import loamstream.pipeline.examples.{PipelineMta, PipelineTargeted}
import loamstream.util.CamelBricksComparer
import loamstream.util.CamelBricksComparer.Diff

/**
 * LoamStream
 * Created by oliverr on 10/8/2015.
 */
object CamelBricksCompareApp extends App {

  def compareCompiled(string: String, pipeline: Pipeline): Seq[CamelBricksComparer.Diff] = {
    CamelBricksComparer.compare(string, CamelBricksCompiler.compile(pipeline))
  }

  def compareMta = compareCompiled(CamelBricksFiles.mtaAsString, PipelineMta)

  def compareTargeted = compareCompiled(CamelBricksFiles.targetedAsString, PipelineTargeted)

  val diffs: Seq[Diff] = compareTargeted
  println("Number of diffs: " + diffs.size)

  for(diff <- diffs) {
    println("Next diff:")
    println(diff.hood1.preLine + diff.hood1.string)
    println(diff.hood2.preLine + diff.hood2.string)
  }
  
  
}
