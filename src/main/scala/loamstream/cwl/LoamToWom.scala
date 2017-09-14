package loamstream.cwl

import lenthall.validation.ErrorOr.ErrorOr
import loamstream.loam.LoamGraph
import loamstream.model.Tool
import wdl4s.wom.callable.{TaskDefinition, WorkflowDefinition}

/**
  * LoamStream
  * Created by oliverr on 9/14/2017.
  */
object LoamToWom {

  def toWom(tool: Tool): ErrorOr[TaskDefinition] = {
    ???
  }

  def toWom(loam: LoamGraph): ErrorOr[WorkflowDefinition] = {
    ???
  }

}
