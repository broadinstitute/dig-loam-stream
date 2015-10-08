package loamstream.metaapps

import loamstream.CamelBricksFiles
import loamstream.compiler.CamelBricksCompiler
import loamstream.pipeline.Pipeline
import loamstream.pipeline.examples.{PipelineMta, PipelineTargeted}
import loamstream.util.CamelBricksComparer

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

  println("Compare mta: " + compareMta.size)

}
