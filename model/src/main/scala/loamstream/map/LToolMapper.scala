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

  object ToolMapping {
    def apply(bindings: Map[Mapping.Slot, Mapping.Target]): ToolMapping = {
      val storeMapping = bindings.collect({ case (PileSlot(pile), StoreTarget(store)) => (pile, store) })
      val toolMapping = bindings.collect({ case (RecipeSlot(recipe), ToolTarget(tool)) => (recipe, tool) })
      ToolMapping(storeMapping, toolMapping)
    }
  }

  case class ToolMapping(stores: Map[LPile, LStore], tools: Map[LRecipe, LTool])

  trait Consumer {
    def foundMapping(mapping: ToolMapping): Unit

    def wantMore: Boolean

    def searchEnded(): Unit
  }

  case class PileSlot(pile: LPile) extends Mapping.Slot {
    println(hashCode + "   " + toString)
  }

  case class RecipeSlot(recipe: LRecipe) extends Mapping.Slot

  case class StoreTarget(store: LStore) extends Mapping.Target

  case class ToolTarget(tool: LTool) extends Mapping.Target

  case class MapMakerConsumer(consumer: Consumer) extends MapMaker.Consumer {
    override def wantsMore: Boolean = consumer.wantMore

    override def solution(node: AriadneNode): Unit = consumer.foundMapping(ToolMapping(node.mapping.bindings))

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

  case class StoreTargetFilter(outputter: Option[LRecipe], inputters: Set[(Int, LRecipe)])
    extends ((Mapping.Target) => Boolean) {
    override def apply(target: Mapping.Target): Boolean = target match {
      case storeTarget: StoreTarget => {
        val outputterCompatible = outputter match {
          case Some(recipe) => storeTarget.store.pile >:> recipe.output
        }
        val inputtersCompatible = inputters.forall({ tup =>
          val (index, recipe) = tup
          storeTarget.store.pile <:< recipe.inputs(index)
        })
        outputterCompatible && inputtersCompatible
      }
      case _ => false
    }
  }

  case class ToolTargetFilter(recipe: LRecipe) extends ((Mapping.Target) => Boolean) {
    override def apply(target: Mapping.Target): Boolean = target match {
      case toolTarget: ToolTarget => toolTarget.tool.recipe <:< recipe
      case _ => false
    }
  }

  case class CompatibilityConstraint(outputters: Map[LPile, LRecipe], inputters: Map[LPile, Set[(Int, LRecipe)]],
                                     pileBounds: Map[LPile, LPile], recipeBounds: Map[LRecipe, LRecipe])
    extends Mapping.Constraint {

    override val slots: Set[Slot] = pileBounds.keySet.map(PileSlot) ++ recipeBounds.keySet.map(RecipeSlot)

    override def slotFilter(slot: Slot): (Target) => Boolean = slot match {
      case PileSlot(slotPile) => StoreTargetFilter(outputters.get(slotPile), inputters.getOrElse(slotPile, Set.empty))
      case RecipeSlot(slotRecipe) => recipeBounds.get(slotRecipe) match {
        case Some(recipeBound) => ToolTargetFilter(recipeBound)
        case None => Function.const(true)
      }
      case _ => Function.const(false)
    }
  }

  object CompatibilityRule extends Mapping.Rule {
    override def constraintFor(slots: Set[Slot], bindings: Map[Slot, Target]): Constraint = {
      val toolMapping = ToolMapping(bindings)
      var pileBounds = Map.empty[LPile, LPile]
      for ((recipe, tool) <- toolMapping.tools) {
        pileBounds += (recipe.output -> tool.recipe.output)
        pileBounds ++= recipe.inputs.zip(tool.recipe.inputs)
      }
      val recipeBounds = Map.empty[LRecipe, LRecipe]
      val outputters = for ((recipe, tool) <- toolMapping.tools) yield (recipe.output, tool.recipe)
      var inputters = Map.empty[LPile, Set[(Int, LRecipe)]]
      for ((recipe, tool) <- toolMapping.tools) {
        ???   // TODO
      }
      CompatibilityConstraint(outputters, inputters, pileBounds, recipeBounds)
    }
  }

  def findSolutions(pipeline: LPipeline, toolBox: LToolBox, consumer: Consumer,
                    strategy: MapMaker.Strategy = MapMaker.NarrowFirstStrategy): Unit = {
    val pileSlots =
      pipeline.calls.map(_.pile).map(pile => (PileSlot(pile), AvailableStores(pile, toolBox.storesFor(pile)))).toMap
    val recipeSlots =
      pipeline.calls.map(_.recipe)
        .map(recipe => (RecipeSlot(recipe), AvailableTools(recipe, toolBox.toolsFor(recipe)))).toMap
    val slots: Map[Mapping.Slot, Mapping.RawChoices] = pileSlots ++ recipeSlots
    val mapping = Mapping.fromSlots(slots)
    MapMaker.traverse(mapping, MapMakerConsumer(consumer))
  }

}
