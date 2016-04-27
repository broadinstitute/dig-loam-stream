package loamstream.model.jobs

import loamstream.map.LToolMapping
import loamstream.model.LPipeline
import loamstream.model.execute.LExecutable
import loamstream.util.shot.Shot
import loamstream.model.Store
import loamstream.model.Tool

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object LToolBox {
  def apply(box: LToolBox, boxes: LToolBox*): LComboToolBox = LComboToolBox(boxes.toSet + box)
}

trait LToolBox {
  def storesFor(pile: Store): Set[Store]

  def toolsFor(recipe: Tool): Set[Tool]

  def createJobs(recipe: Tool, pipeline: LPipeline, mapping: LToolMapping): Shot[Set[LJob]]

  def createExecutable(pipeline: LPipeline, mapping: LToolMapping): LExecutable

  def ++(oBox: LToolBox): LComboToolBox = oBox match {
    case LComboToolBox(boxes) => LComboToolBox(boxes + this)
    case _ => LToolBox(this, oBox)
  }
}
