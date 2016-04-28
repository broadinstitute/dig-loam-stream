package loamstream.model.jobs

import loamstream.model.LPipeline
import loamstream.model.Tool
import loamstream.model.execute.LExecutable
import loamstream.util.Shot

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object LToolBox {
  def apply(box: LToolBox, boxes: LToolBox*): LComboToolBox = LComboToolBox(boxes.toSet + box)
}

trait LToolBox {
  def createJobs(recipe: Tool, pipeline: LPipeline): Shot[Set[LJob]]

  def createExecutable(pipeline: LPipeline): LExecutable

  def ++(oBox: LToolBox): LComboToolBox = oBox match {
    case LComboToolBox(boxes) => LComboToolBox(boxes + this)
    case _ => LToolBox(this, oBox)
  }
}
