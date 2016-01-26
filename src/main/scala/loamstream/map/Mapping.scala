package loamstream.map

import loamstream.map.Mapping.{ConstraintMaker, Constraint, Slot, Target, TargetChoices}

/**
  * LoamStream
  * Created by oliverr on 1/22/2016.
  */
object Mapping {

  trait Target

  trait Slot

  trait TargetChoices {
    def numMoreTargets: Int

    def nextTarget: Target

    def withConstraint(constraint: Constraint): TargetChoices
  }

  trait ConstraintMaker {
    def newConstraintFor(mapping: Mapping): Constraint
  }

  trait Constraint {
    def mapping: Mapping

    def slots: Set[Slot]

    def slotFilter(slot: Slot): Target => Boolean
  }

  def empty = Mapping(Set.empty, Set.empty, Map.empty, Set.empty, Map.empty)

  def fromSlots(slots: Map[Slot, TargetChoices]) =
    Mapping(slots.keySet, slots.keySet, slots, Set.empty, Map.empty)

}

case class Mapping(slots: Set[Slot], emptySlots: Set[Slot], choices: Map[Slot, TargetChoices],
                   constraints: Set[Constraint], bindings: Map[Slot, Target]) {

  def plusSlot(slot: Slot, slotChoices: TargetChoices) =
    copy(slots = slots + slot, emptySlots = emptySlots + slot, choices = choices + (slot -> slotChoices))

  def plusConstraint(constraintMaker: ConstraintMaker) = {
    val constraint = constraintMaker.newConstraintFor(this)
    val reducedChoices = constraint.slots.map(slot => (slot, choices(slot).withConstraint(constraint))).toMap
    copy(constraints = constraints + constraint, choices = choices ++ reducedChoices)
  }

  def plusBinding(slot: Slot, target: Target) =
    copy(emptySlots = emptySlots - slot, bindings = bindings + (slot -> target))

}
