package loamstream.model.jobs

import loamstream.model.LPipeline
import loamstream.model.Tool
import loamstream.model.execute.LExecutable
import loamstream.util.Shot
import loamstream.model.AST

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
trait LToolBox {
  def createJobs(tool: Tool, pipeline: LPipeline): Shot[Set[LJob]]

  def createExecutable(pipeline: LPipeline): LExecutable

  def createExecutable(ast: AST): LExecutable
  
  def ++(oBox: LToolBox): LComboToolBox = oBox match {
    case LComboToolBox(boxes) => LComboToolBox(boxes + this)
    case _ => LToolBox(this, oBox)
  }
}

object LToolBox {
  def apply(box: LToolBox, boxes: LToolBox*): LComboToolBox = LComboToolBox(boxes.toSet + box)
}