package loamstream.map

import loamstream.map.Mapping.{Constraint, Slot, Target}
import loamstream.model.LPipeline
import loamstream.model.jobs.LToolBox
import loamstream.model.jobs.tools.LTool
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore
import util.Iterative
import util.Iterative.SizePredicting

/**
 * LoamStream
 * Created by oliverr on 2/17/2016.
 */
object LToolMapper {

  case class ToolMapping(stores: Map[LPile, LStore], tools: Map[LRecipe, LTool])

  trait Consumer {
    def foundMapping(mapping: ToolMapping): Unit

    def wantMore: Boolean

    def searchEnded(): Unit
  }

  case class PileSlot(pile: LPile) extends Mapping.Slot

  case class RecipeSlot(recipe: LRecipe) extends Mapping.Slot

  case class StoreTarget(store: LStore) extends Mapping.Target

  case class ToolTarget(tool: LTool) extends Mapping.Target

  case class MapMakerConsumer(consumer: Consumer) extends MapMaker.Consumer {
    override def wantsMore: Boolean = consumer.wantMore

    override def solution(node: AriadneNode): Unit = {
      val storeMapping =
        node.mapping.bindings.collect({ case (PileSlot(pile), StoreTarget(store)) => (pile, store) })
      val toolMapping =
        node.mapping.bindings.collect({ case (RecipeSlot(recipe), ToolTarget(tool)) => (recipe, tool) })
      consumer.foundMapping(ToolMapping(storeMapping, toolMapping))
    }

    override def step(node: AriadneNode): Unit = ()

    override def end(): Unit = consumer.searchEnded()
  }

  case class AvailableStores(pile: LPile, stores: Set[LStore]) extends Mapping.RawChoices {
    override def constrainedBy(slot: Slot, slotConstraints: Set[Constraint]): SizePredicting[Target] = {
      var remainingTargets: Set[Target] = stores.map(StoreTarget)
      for (slotConstraint <- slotConstraints) {
        remainingTargets = remainingTargets.filter(slotConstraint.slotFilter(slot))
      }
      Iterative.SetBased(remainingTargets)
    }
  }

  case class AvailableTools(recipe: LRecipe, tools: Set[LTool]) extends Mapping.RawChoices {
    override def constrainedBy(slot: Slot, slotConstraints: Set[Constraint]): SizePredicting[Target] = {
      var remainingTargets: Set[Target] = tools.map(ToolTarget)
      for (slotConstraint <- slotConstraints) {
        remainingTargets = remainingTargets.filter(slotConstraint.slotFilter(slot))
      }
      Iterative.SetBased(remainingTargets)
    }
  }

  def findSolutions(pipeline: LPipeline, toolBox: LToolBox, consumer: Consumer,
                    strategy: MapMaker.Strategy = MapMaker.NarrowFirstStrategy): Unit = {
    val pileSlots =
      pipeline.calls.map(_.pile).map(pile => (PileSlot(pile), AvailableStores(pile, toolBox.storesFor(pile)))).toMap
    val recipeSlots =
      pipeline.calls.map(_.recipe)
        .map(recipe => (RecipeSlot(recipe), AvailableTools(recipe, toolBox.toolsFor(recipe)))).toMap
    val slots : Map[Mapping.Slot, Mapping.RawChoices] = pileSlots ++ recipeSlots
    val mapping = Mapping.fromSlots(slots)
    for(slot <- mapping.slots) {
      println("1")
      println(mapping.rawChoices(slot))
      println("2")
    }
    MapMaker.traverse(mapping, MapMakerConsumer(consumer))
  }

}
