package loamstream.model.jobs

import loamstream.map.LToolMapping
import loamstream.model.LPipeline
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.tools.LTool
import loamstream.model.recipes.LRecipe
import loamstream.model.StoreBase
import loamstream.util.shot.{Miss, Shot}

/**
  * LoamStream
  * Created by oliverr on 3/28/2016.
  */
case class LComboToolBox(boxes: Set[LToolBox]) extends LToolBox {
  override def storesFor(pile: StoreBase): Set[StoreBase] = boxes.flatMap(box => box.storesFor(pile))

  override def toolsFor(recipe: LRecipe): Set[LTool] = boxes.flatMap(box => box.toolsFor(recipe))

  val noBoxesErrorMessage = "This combination toolbox contains no toolboxes."

  override def createJobs(recipe: LRecipe, pipeline: LPipeline, mapping: LToolMapping): Shot[Set[LJob]] =
    boxes.map(box => box.createJobs(recipe, pipeline, mapping)).reduceOption(_.orElse(_)) match {
      case Some(shot) => shot
      case None => Miss(noBoxesErrorMessage)
    }

  override def createExecutable(pipeline: LPipeline, mapping: LToolMapping): LExecutable =
    LExecutable(boxes.flatMap(box => box.createExecutable(pipeline, mapping).jobs))

  override def ++(oBox: LToolBox): LComboToolBox = oBox match {
    case LComboToolBox(oBoxes) => LComboToolBox(oBoxes ++ boxes)
    case _ => LComboToolBox(boxes + oBox)
  }

}
