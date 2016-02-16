package loamstream.map

import loamstream.map.Mapping.{Slot, Target}
import util.Iterative

/**
  * LoamStream
  * Created by oliverr on 1/27/2016.
  */
object AriadneNode {

  def fromMapping(mapping: Mapping) = RootWithoutSlot(mapping)

  trait Root extends AriadneNode {
    def parentOpt = None

    def nextAncestorWithOtherChoices: Option[AriadneNode.WithSlot] = None
  }

  trait Child extends AriadneNode {
    def parent: AriadneNode.WithSlot

    def parentOpt = Some(parent)

    def nextAncestorWithOtherChoices: Option[AriadneNode.WithSlot] = {
      val choices = parent.choices
      if (choices.nonEmpty && choices.tail.nonEmpty) Some(parent) else parent.nextAncestorWithOtherChoices
    }

  }

  trait WithoutSlot extends AriadneNode {
    def slotOpt = None

    def pickSlot(slot: Slot): WithSlot

    def choicesOpt = None

    def isSolution: Boolean = mapping.unboundSlots.isEmpty
  }

  trait WithSlot extends AriadneNode {
    def slot: Slot

    def slotOpt = Some(slot)

    def choices: Iterative[Target]

    def choicesOpt = Some(choices)

    def bind: ChildWithoutSlot

    def withNextChoice: WithSlot

    def isSolution: Boolean = false
  }

  case class RootWithoutSlot(mapping: Mapping) extends Root with WithoutSlot {
    override def pickSlot(slot: Slot): RootWithSlot =
      RootWithSlot(mapping, slot, mapping.choices(slot))
  }

  case class RootWithSlot(mapping: Mapping, slot: Slot, choices: Iterative[Target]) extends Root with WithSlot {
    override def bind: ChildWithoutSlot = ChildWithoutSlot(mapping.plusBinding(slot, choices.head), this)

    override def withNextChoice: WithSlot = RootWithSlot(mapping, slot, choices.tail)
  }

  case class ChildWithoutSlot(mapping: Mapping, parent: AriadneNode.WithSlot)
    extends Child with WithoutSlot {
    override def pickSlot(slot: Slot): ChildWithSlot =
      ChildWithSlot(mapping, parent, slot, mapping.choices(slot))
  }

  case class ChildWithSlot(mapping: Mapping, parent: AriadneNode.WithSlot, slot: Slot, choices: Iterative[Target])
    extends Child with WithSlot {
    override def bind: ChildWithoutSlot = ChildWithoutSlot(mapping.plusBinding(slot, choices.head), this)

    override def withNextChoice: WithSlot = ChildWithSlot(mapping, parent, slot, choices.tail)
  }

}

trait AriadneNode {
  def mapping: Mapping

  def parentOpt: Option[AriadneNode.WithSlot]

  def slotOpt: Option[Slot]

  def choicesOpt: Option[Iterative[Target]]

  def hasChoice: Boolean = choicesOpt.flatMap(_.headOpt).nonEmpty

  def hasOtherChoices: Boolean = choicesOpt.flatMap(_.tail.headOpt).nonEmpty

  def isSolution: Boolean

  def nextAncestorWithOtherChoices: Option[AriadneNode.WithSlot]
}

