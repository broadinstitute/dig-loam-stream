package loamstream.model

/**
 * LoamStream
 * Created by oliverr on 2/17/2016.
 */
trait LPipeline {
  def stores: Set[Store]

  def tools: Set[Tool]

  lazy val toolsByOutput: Map[Store, Set[Tool]] = {
    val outputsToTools = for {
      tool <- tools.toSeq
      (outputId, output) <- tool.outputs
    } yield output -> tool

    val z: Map[Store, Set[Tool]] = Map.empty

    outputsToTools.foldLeft(z) { (map, tuple) =>
      val (output, tool) = tuple

      val newTools = map.get(output) match {
        case None        => Set(tool)
        case Some(tools) => tools + tool
      }

      map + (output -> newTools)
    }
  }
}
