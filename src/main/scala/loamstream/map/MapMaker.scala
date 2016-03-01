package loamstream.map

import loamstream.map.AriadneNode.WithoutSlot
import loamstream.map.Mapping.Slot
import loamstream.util.snag.Snag
import loamstream.util.shot.{Hit, Miss, Shot}

/**
  * LoamStream
  * Created by oliverr on 1/27/2016.
  */
object MapMaker {

  sealed trait Tactic

  case class Descend(slot: Slot) extends Tactic

  case object Solution extends Tactic

  case object DoNotDescend extends Tactic

  case object Terminate extends Tactic

  trait Strategy {
    def forNode(node: AriadneNode.WithoutSlot): Tactic
  }

  object NarrowFirstStrategy extends Strategy {
    override def forNode(node: WithoutSlot): Tactic = {
      val availableSlots = node.mapping.unboundSlots
      if (availableSlots.nonEmpty) {
        var bestSlot = availableSlots.head
        var nBestSlotChoices = node.mapping.choices(bestSlot).predictedSize
        val slotIterator = availableSlots.iterator
        while (slotIterator.hasNext && nBestSlotChoices > 0) {
          val slot = slotIterator.next()
          val nSlotChoices = node.mapping.choices(slot).predictedSize
          if (nSlotChoices < nBestSlotChoices) {
            bestSlot = slot
            nBestSlotChoices = nSlotChoices
          }
        }
        if (nBestSlotChoices > 0) {
          Descend(bestSlot)
        } else {
          DoNotDescend
        }
      } else {
        Solution
      }
    }
  }

  trait Consumer {
    def wantsMore: Boolean

    def step(node: AriadneNode): Unit

    def solution(node: AriadneNode): Unit

    def end(): Unit
  }

  def backtrack(node: WithoutSlot): Option[WithoutSlot] = {
    node match {
      case child: AriadneNode.Child =>
        child.nextAncestorWithOtherChoices match {
          case Some(ancestor) => Some(ancestor.withNextChoice.bind)
          case _ => None
        }
      case _ => None
    }
  }

  case class TraversalState(node: WithoutSlot, keepGoingTactic: Boolean)

  def traverseDoNotDescend(state: TraversalState): TraversalState = {
    backtrack(state.node) match {
      case Some(nodeNew) => state.copy(node = nodeNew)
      case None => state.copy(keepGoingTactic = false)
    }
  }

  def traverseSolution(state: TraversalState, consumer: Consumer): TraversalState = {
    consumer.solution(state.node)
    if (consumer.wantsMore) {
      backtrack(state.node) match {
        case Some(nodeNew) => state.copy(node = nodeNew)
        case None => state.copy(keepGoingTactic = false)
      }
    } else {
      state.copy(keepGoingTactic = false)
    }
  }

  def traverse(mapping: Mapping, consumer: Consumer, strategy: Strategy = NarrowFirstStrategy): Unit = {
    val keepGoingTacticInitial = true
    var state = TraversalState(AriadneNode.fromMapping(mapping), keepGoingTacticInitial)
    while (state.keepGoingTactic && consumer.wantsMore) {
      consumer.step(state.node)
      strategy.forNode(state.node) match {
        case Descend(slot) => state = state.copy(node = state.node.pickSlot(slot).bind)
        case DoNotDescend => state = traverseDoNotDescend(state)
        case Solution => state = traverseSolution(state, consumer)
        case Terminate => state = state.copy(keepGoingTactic = false)
      }
    }
    consumer.end()
  }

  def findBestSlotToBind(ariadneNode: AriadneNode.WithoutSlot): (Slot, Int) = {
    val availableSlots = ariadneNode.mapping.unboundSlots
    var bestSlot = availableSlots.head
    var nBestSlotChoices = ariadneNode.mapping.choices(bestSlot).predictedSize
    for (slot <- availableSlots) {
      val nSlotChoices = ariadneNode.mapping.choices(slot).predictedSize
      if (nSlotChoices < nBestSlotChoices) {
        bestSlot = slot
        nBestSlotChoices = nSlotChoices
      }
    }
    (bestSlot, nBestSlotChoices)
  }

  def pickBestSlot(node: AriadneNode.WithoutSlot): Shot[AriadneNode.WithSlot] = {
    val (slot, nSlotChoices) = findBestSlotToBind(node)
    if (nSlotChoices > 0) {
      Hit(node.pickSlot(slot))
    } else {
      Miss(Snag("No more choice available."))
    }
  }

  def bindBestSlot(node: AriadneNode.WithoutSlot): Shot[AriadneNode.WithoutSlot] =
    pickBestSlot(node).map(_.bind)

  def shootAtBinding(mapping: Mapping): Shot[Mapping] = {
    val ariadneNode = AriadneNode.fromMapping(mapping)
    bindBestSlot(ariadneNode).map(_.mapping)
  }

  def shootAtBinding(mapping: Mapping, nBindings: Int): Shot[Mapping] = {
    var ariadneNodeShot: Shot[AriadneNode.WithoutSlot] = Hit(AriadneNode.fromMapping(mapping))
    for (i <- 1 to nBindings) {
      ariadneNodeShot = ariadneNodeShot.flatMap(bindBestSlot)
    }
    ariadneNodeShot.map(_.mapping)
  }

}
