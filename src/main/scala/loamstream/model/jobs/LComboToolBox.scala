package loamstream.model.jobs

import loamstream.model.execute.LExecutable
import loamstream.model.{AST, LPipeline, Tool}
import loamstream.util.{Miss, Shot, Shots}

/**
  * LoamStream
  * Created by oliverr on 3/28/2016.
  */
case class LComboToolBox(boxes: Set[LToolBox]) extends LToolBox {
  private val noBoxesErrorMessage = "This combination toolbox contains no toolboxes."

  override def createJobs(tool: Tool, pipeline: LPipeline): Shot[Set[LJob]] = {
    boxes.map(box => box.createJobs(tool, pipeline)).reduceOption(_.orElse(_)) match {
      case Some(shot) => shot
      case None => Miss(noBoxesErrorMessage)
    }
  }

  override def createExecutable(pipeline: LPipeline): LExecutable = {
    LExecutable(boxes.flatMap(box => box.createExecutable(pipeline).jobs))
  }

  override def ++(oBox: LToolBox): LComboToolBox = oBox match {
    case LComboToolBox(oBoxes) => LComboToolBox(oBoxes ++ boxes)
    case _ => LComboToolBox(boxes + oBox)
  }

  override def toolToJobShot(tool: Tool): Shot[LJob] = Shots.findHit[LToolBox, LJob](boxes, _.toolToJobShot(tool))
}
