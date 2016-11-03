package loamstream.model

import loamstream.util.TypeBox
/**
  * LoamStream
  * Created by oliverr on 02/26/2016.
  */
final case class ToolSpec(inputs: Map[LId, TypeBox.Untyped], outputs: Map[LId, TypeBox.Untyped])

